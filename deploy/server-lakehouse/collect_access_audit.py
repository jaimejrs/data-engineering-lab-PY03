"""Coleta auditoria de acesso ao servidor: sessões SSH (login/logout, via
journalctl) e comando executado por usuário logado (via auditd/ausearch) —
"quem entrou, quanto tempo ficou, o que rodou" pras 4 pessoas com acesso ao
servidor. Roda direto no host via cron (mesmo padrão de
collect_infra_metrics.py), sem driver Python nem `.env`: grava via `docker
exec ... trino --execute`, usando o `trino` CLI que já existe dentro do
container do Trino.

`ausearch` é filtrado por regra do auditd (auid setado, ver
deploy/server-lakehouse/README.md) — exclui processo interno de
container/cron que nunca passou por login, só sobra comando de sessão
interativa real (inclusive via sudo, auid persiste através de sudo).

Idempotente via arquivo de estado (~/.access-audit-collector-state.json,
mesmo princípio do auto-sync.py): cada execução só busca desde o último
timestamp processado, não reprocessa o mesmo evento duas vezes.

Uso: `python3 collect_access_audit.py [nome_do_container_trino]`
"""

import json
import re
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_TRINO_CONTAINER = "lakehouse_trino"
CATALOG_SCHEMA = "iceberg.audit"
STATE_FILE = Path.home() / ".access-audit-collector-state.json"

DDL_SESSOES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.sessoes_ssh (
    evento varchar,
    usuario varchar,
    origem_ip varchar,
    sessao_pid varchar,
    timestamp_evento timestamp,
    coletado_em timestamp
)
"""

DDL_COMANDOS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.comandos_executados (
    usuario varchar,
    executado_como varchar,
    comando varchar,
    executavel varchar,
    sessao_id varchar,
    tty varchar,
    sucesso boolean,
    timestamp_evento timestamp,
    coletado_em timestamp
)
"""

LOGIN_RE = re.compile(r"sshd-session\[(?P<pid>\d+)\]: Accepted \S+ for (?P<user>\S+) from (?P<ip>\S+) port \d+")
SESSION_CLOSED_RE = re.compile(
    r"sshd-session\[(?P<pid>\d+)\]: pam_unix\(sshd:session\): session closed for user (?P<user>\S+)"
)
JOURNAL_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")


def run(args):
    return subprocess.run(args, capture_output=True, text=True)


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state))


def collect_ssh_sessions(since_iso):
    """Eventos de login/logout SSH desde `since_iso` (ISO 8601) — None busca tudo disponível no journal."""
    args = ["journalctl", "-u", "ssh", "--no-pager", "-o", "short-iso"]
    if since_iso:
        args += ["--since", since_iso]
    out = run(args).stdout
    rows = []
    for line in out.splitlines():
        ts_match = JOURNAL_TS_RE.match(line)
        if not ts_match:
            continue
        ts = ts_match.group(1).replace("T", " ")
        m = LOGIN_RE.search(line)
        if m:
            rows.append(
                {
                    "evento": "login",
                    "usuario": m.group("user"),
                    "origem_ip": m.group("ip"),
                    "sessao_pid": m.group("pid"),
                    "timestamp_evento": ts,
                }
            )
            continue
        m = SESSION_CLOSED_RE.search(line)
        if m:
            rows.append(
                {
                    "evento": "logout",
                    "usuario": m.group("user"),
                    "origem_ip": None,
                    "sessao_pid": m.group("pid"),
                    "timestamp_evento": ts,
                }
            )
    return rows


def _parse_audit_ts(raw):
    """'07/26/26 13:55:13.347' -> '2026-07-26 13:55:13.347' (ISO, ano com 4 dígitos)."""
    date_part, time_part = raw.split(" ", 1)
    mm, dd, yy = date_part.split("/")
    return f"20{yy}-{mm}-{dd} {time_part}"


def collect_command_executions(since_mmddyy=None, since_hhmmss=None):
    """Comandos executados desde `since_mmddyy`/`since_hhmmss` (formato ausearch,
    'MM/DD/YY' e 'HH:MM:SS') — ambos None busca tudo disponível no backlog do auditd.
    """
    # ausearch exige ler /var/log/audit/audit.log — via sudo (regra dedicada,
    # NOPASSWD só para /usr/sbin/ausearch, ver deploy/server-lakehouse/README.md).
    # `ausearch` trava indefinidamente sem um terminal controlador (reproduzido:
    # roda na hora via SSH interativo/pty, mas pendura para sempre via cron ou
    # `exec_command` sem pty) — `script -qc "..." /dev/null` aloca um
    # pseudo-terminal só pra isso, sem mudar nada do resultado.
    ausearch_cmd = ["sudo", "-n", "/usr/sbin/ausearch", "-k", "cmd_exec", "-i"]
    if since_mmddyy and since_hhmmss:
        ausearch_cmd += ["-ts", since_mmddyy, since_hhmmss]
    else:
        ausearch_cmd += ["-ts", "boot"]
    args = ["script", "-qc", shlex.join(ausearch_cmd), "/dev/null"]
    result = run(args)
    if result.returncode != 0:
        if "no matches" in (result.stdout + result.stderr).lower():
            return []
        # Falha real (ex: sudo negado, ausearch quebrado) — melhor parar o
        # coletor de vez do que seguir com lista vazia e marcar o estado como
        # "processado" silenciosamente (perderia o intervalo pra sempre).
        raise RuntimeError(f"ausearch falhou (exit={result.returncode}): {result.stderr or result.stdout}")
    out = result.stdout

    rows = []
    for block in out.split("----"):
        proctitle_m = re.search(r"type=PROCTITLE msg=audit\([^)]*\)\s*:\s*proctitle=(.+)", block)
        syscall_m = re.search(
            r"type=SYSCALL msg=audit\((?P<ts>[\d/]+ [\d:.]+):\d+\)\s*:.*?"
            r"success=(?P<success>\S+).*?"
            r"auid=(?P<auid>\S+)\s.*?euid=(?P<euid>\S+)\s.*?"
            r"tty=(?P<tty>\S+)\s+ses=(?P<ses>\S+)\s+comm=(?P<comm>\S+)\s+exe=(?P<exe>\S+)",
            block,
        )
        if not syscall_m:
            continue
        auid = syscall_m.group("auid")
        if auid in ("unset", "unknown(4294967295)", "4294967295"):
            continue
        comando = proctitle_m.group(1).strip() if proctitle_m else syscall_m.group("comm")
        rows.append(
            {
                "usuario": auid,
                "executado_como": syscall_m.group("euid"),
                "comando": comando,
                "executavel": syscall_m.group("exe"),
                "sessao_id": syscall_m.group("ses"),
                "tty": syscall_m.group("tty"),
                "sucesso": syscall_m.group("success") == "yes",
                "timestamp_evento": _parse_audit_ts(syscall_m.group("ts")),
            }
        )
    return rows


def _sql_str(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def build_insert_sessoes(rows, coletado_em):
    values = ", ".join(
        f"({_sql_str(r['evento'])}, {_sql_str(r['usuario'])}, {_sql_str(r['origem_ip'])}, "
        f"{_sql_str(r['sessao_pid'])}, TIMESTAMP {_sql_str(r['timestamp_evento'])}, TIMESTAMP {_sql_str(coletado_em)})"
        for r in rows
    )
    return f"INSERT INTO {CATALOG_SCHEMA}.sessoes_ssh VALUES {values}"


def build_insert_comandos(rows, coletado_em):
    values = ", ".join(
        f"({_sql_str(r['usuario'])}, {_sql_str(r['executado_como'])}, {_sql_str(r['comando'])}, "
        f"{_sql_str(r['executavel'])}, {_sql_str(r['sessao_id'])}, {_sql_str(r['tty'])}, "
        f"{'true' if r['sucesso'] else 'false'}, TIMESTAMP {_sql_str(r['timestamp_evento'])}, "
        f"TIMESTAMP {_sql_str(coletado_em)})"
        for r in rows
    )
    return f"INSERT INTO {CATALOG_SCHEMA}.comandos_executados VALUES {values}"


def trino_execute(sql, trino_container):
    run(["docker", "exec", trino_container, "trino", "--execute", sql])


# Lote pequeno — cada INSERT vira um argumento de linha de comando pro
# `docker exec` (ver run()); um backlog grande (ex: 1ª execução, sem estado
# ainda) pode gerar SQL grande o bastante pra estourar ARG_MAX do SO
# ("Argument list too long").
CHUNK_SIZE = 200


def _insert_in_chunks(rows, build_fn, coletado_em, trino_container):
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        trino_execute(build_fn(chunk, coletado_em), trino_container)


def main(trino_container=DEFAULT_TRINO_CONTAINER):
    coletado_em = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    trino_execute(DDL_SESSOES, trino_container)
    trino_execute(DDL_COMANDOS, trino_container)

    state = load_state()
    now = datetime.now(timezone.utc)

    # Cada fonte salva seu próprio progresso assim que termina — se
    # collect_command_executions falhar (ver RuntimeError acima), sessões já
    # processadas não precisam ser reprocessadas/duplicadas na próxima
    # execução; só o comando fica pendente de retry.
    sessoes = collect_ssh_sessions(state.get("ultimo_journal_iso"))
    if sessoes:
        _insert_in_chunks(sessoes, build_insert_sessoes, coletado_em, trino_container)
    state["ultimo_journal_iso"] = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    save_state(state)

    comandos = collect_command_executions(state.get("ultimo_audit_mmddyy"), state.get("ultimo_audit_hhmmss"))
    if comandos:
        _insert_in_chunks(comandos, build_insert_comandos, coletado_em, trino_container)
    state["ultimo_audit_mmddyy"] = now.strftime("%m/%d/%y")
    state["ultimo_audit_hhmmss"] = now.strftime("%H:%M:%S")
    save_state(state)

    print(f"Coletado: {len(sessoes)} eventos de sessão SSH, {len(comandos)} comandos executados.")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TRINO_CONTAINER)
