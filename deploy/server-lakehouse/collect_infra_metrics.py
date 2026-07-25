"""Coleta CPU/memória por container e uso de disco do host, grava em Iceberg
via Trino — item "Métricas de infraestrutura" de docs/03-pendencias-e-
melhorias.md. Roda direto no host via cron (mesmo padrão de auto-sync.py),
sem montar o socket do Docker em nenhum container: `docker stats`/`df` são
chamados como CLI do host, e a gravação usa o `trino` CLI que já existe
dentro do container do Trino (`docker exec ... trino --execute`) — não
precisa de driver Python nem `.env` no host.

Uso: `python3 collect_infra_metrics.py [nome_do_container_trino]`
(padrão: lakehouse_trino, o nome no overlay do servidor).
"""

import json
import re
import subprocess
import sys
from datetime import datetime, timezone

DEFAULT_TRINO_CONTAINER = "lakehouse_trino"
CATALOG_SCHEMA = "iceberg.gold"

DDL_CONTAINERS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.infra_metricas_containers (
    container varchar,
    cpu_percent double,
    mem_usage_bytes bigint,
    mem_limit_bytes bigint,
    mem_percent double,
    coletado_em timestamp
)
"""

DDL_DISCO = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.infra_metricas_disco (
    ponto_montagem varchar,
    total_bytes bigint,
    usado_bytes bigint,
    pct_usado double,
    coletado_em timestamp
)
"""

# Docker relata memória em GiB/MiB/... (potência de 2) mesmo usando o sufixo
# "GB"/"MB" às vezes dependendo da versão — trata os dois como base 1024 pra
# não subestimar (a diferença de base é irrelevante no uso real, que é olhar
# tendência/alerta, não auditoria de byte exato).
UNIT_MULTIPLIERS = {
    "b": 1,
    "kib": 1024,
    "kb": 1024,
    "mib": 1024**2,
    "mb": 1024**2,
    "gib": 1024**3,
    "gb": 1024**3,
    "tib": 1024**4,
    "tb": 1024**4,
}


def run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, check=True)


def parse_size(text: str) -> int:
    """Converte '1.23GiB' -> bytes. Formato inválido/vazio vira 0."""
    match = re.match(r"([\d.]+)\s*([a-zA-Z]+)", text.strip())
    if not match:
        return 0
    value, unit = match.groups()
    multiplier = UNIT_MULTIPLIERS.get(unit.lower(), 1)
    return int(float(value) * multiplier)


def parse_percent(text: str) -> float:
    """Converte '12.34%' -> 12.34. Vazio/inválido vira 0.0."""
    text = text.strip().rstrip("%")
    return float(text) if text else 0.0


def collect_container_stats() -> list[dict]:
    """Uma linha JSON por container (`docker stats --no-stream --format {{json .}}`)."""
    output = run(["docker", "stats", "--no-stream", "--format", "{{json .}}"]).stdout
    rows = []
    for line in output.strip().splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        mem_used_str, _, mem_limit_str = data["MemUsage"].partition("/")
        rows.append(
            {
                "container": data["Name"],
                "cpu_percent": parse_percent(data["CPUPerc"]),
                "mem_usage_bytes": parse_size(mem_used_str),
                "mem_limit_bytes": parse_size(mem_limit_str),
                "mem_percent": parse_percent(data["MemPerc"]),
            }
        )
    return rows


def collect_disk_stats() -> list[dict]:
    """Uso de disco por ponto de montagem real do host (exclui tmpfs/overlay/squashfs
    — filesystems virtuais que não representam disco físico do servidor)."""
    output = run(
        [
            "df",
            "-B1",
            "--output=target,size,used,pcent",
            "-x",
            "tmpfs",
            "-x",
            "overlay",
            "-x",
            "squashfs",
        ]
    ).stdout
    rows = []
    for line in output.strip().splitlines()[1:]:  # pula cabeçalho
        parts = line.split()
        if len(parts) != 4:
            continue
        target, total, used, pcent = parts
        total_bytes = int(total)
        # Pseudo-filesystems reais (ex: /sys/firmware/efi/efivars, ~200KB) não
        # são "-x"-áveis por tipo sem enumerar cada um — um piso de tamanho é
        # mais simples e cobre o mesmo caso: nenhum disco físico do servidor
        # tem menos de 1GB.
        if total_bytes < 1_000_000_000:
            continue
        rows.append(
            {
                "ponto_montagem": target,
                "total_bytes": total_bytes,
                "usado_bytes": int(used),
                "pct_usado": float(pcent.rstrip("%")),
            }
        )
    return rows


def trino_execute(sql: str, trino_container: str) -> None:
    run(["docker", "exec", trino_container, "trino", "--execute", sql])


def build_insert_containers(rows: list[dict], coletado_em: str) -> str:
    values = ", ".join(
        f"('{r['container']}', {r['cpu_percent']}, {r['mem_usage_bytes']}, "
        f"{r['mem_limit_bytes']}, {r['mem_percent']}, TIMESTAMP '{coletado_em}')"
        for r in rows
    )
    return f"INSERT INTO {CATALOG_SCHEMA}.infra_metricas_containers VALUES {values}"


def build_insert_disco(rows: list[dict], coletado_em: str) -> str:
    values = ", ".join(
        f"('{r['ponto_montagem']}', {r['total_bytes']}, {r['usado_bytes']}, "
        f"{r['pct_usado']}, TIMESTAMP '{coletado_em}')"
        for r in rows
    )
    return f"INSERT INTO {CATALOG_SCHEMA}.infra_metricas_disco VALUES {values}"


def main(trino_container: str = DEFAULT_TRINO_CONTAINER) -> None:
    coletado_em = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    trino_execute(DDL_CONTAINERS, trino_container)
    trino_execute(DDL_DISCO, trino_container)

    containers = collect_container_stats()
    if containers:
        trino_execute(build_insert_containers(containers, coletado_em), trino_container)

    disks = collect_disk_stats()
    if disks:
        trino_execute(build_insert_disco(disks, coletado_em), trino_container)

    print(f"Coletado: {len(containers)} containers, {len(disks)} pontos de montagem.")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TRINO_CONTAINER)
