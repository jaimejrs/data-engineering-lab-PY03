#!/usr/bin/env python3
"""Deploy automático — só aplica `sync-from-git.sh --apply` se `main` tiver um
commit novo E a CI (GitHub Actions) desse commit já estiver verde.

Desenho deliberado (item 12 de docs/06-analise-critica.md, "ainda manual"):
o SERVIDOR pergunta ao GitHub, em vez do GitHub empurrar pro servidor. Isso
evita duas coisas arriscadas num servidor compartilhado: guardar uma chave
SSH nos segredos do GitHub Actions (superfície de ataque se o repositório ou
a Action forem comprometidos) e instalar um runner residente do GitHub no
servidor. O repositório é público, então a Checks API é lida sem token.

Rodado por cron (ver deploy/server-lakehouse/README.md) — cada execução é
barata (1 chamada HTTP) quando não há commit novo. Nunca aplica com CI
pendente ou vermelho; nunca reaplica o mesmo commit duas vezes (state file);
sempre loga o diff em dry-run antes de aplicar de verdade, e confere
`airflow dags list-import-errors` depois — para dar rastro auditável de um
processo que agora roda sem humano olhando.

Uso manual (fora do cron, para testar): `python3 auto-sync.py`
"""

import datetime
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO = "jaimejrs/data-engineering-lab-PY03"
HOME = Path.home()
SYNC_SCRIPT = HOME / "sync-from-git.sh"
STATE_FILE = HOME / ".auto-sync-last-sha"
LOG_FILE = HOME / "auto-sync.log"


def log(msg: str) -> None:
    line = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600)


def remote_sha() -> str | None:
    out = run(["git", "ls-remote", f"https://github.com/{REPO}", "refs/heads/main"])
    if out.returncode != 0 or not out.stdout.strip():
        log(f"git ls-remote falhou (exit={out.returncode}): {out.stderr.strip()}")
        return None
    return out.stdout.split()[0]


def ci_passed(sha: str) -> bool:
    url = f"https://api.github.com/repos/{REPO}/commits/{sha}/check-runs"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json", "User-Agent": "auto-sync"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.load(resp)
    except (OSError, json.JSONDecodeError) as exc:
        log(f"Falha consultando a Checks API: {exc}")
        return False

    runs = data.get("check_runs", [])
    if not runs:
        log(f"Nenhum check run publicado ainda para {sha[:10]} — aguardando próximo ciclo")
        return False

    pending = [r["name"] for r in runs if r["status"] != "completed"]
    if pending:
        log(f"CI ainda rodando para {sha[:10]}: {pending} — aguardando próximo ciclo")
        return False

    failed = [r["name"] for r in runs if r["conclusion"] != "success"]
    if failed:
        log(f"CI vermelho para {sha[:10]}: {failed} — NÃO aplicando")
        return False

    log(f"CI verde para {sha[:10]} ({len(runs)} checks)")
    return True


def sync(apply: bool) -> subprocess.CompletedProcess:
    args = [str(SYNC_SCRIPT)] + (["--apply"] if apply else [])
    return run(args)


def post_deploy_healthcheck() -> None:
    out = run(["docker", "exec", "datalab_airflow_scheduler", "airflow", "dags", "list-import-errors"])
    text = (out.stdout or "").strip()
    if "No data found" in text or text == "":
        log("Healthcheck: nenhum erro de import de DAG após o deploy")
    else:
        log(f"ALERTA — erro de import de DAG após o deploy, revisar manualmente:\n{text}")


def main() -> None:
    sha = remote_sha()
    if not sha:
        return

    last = STATE_FILE.read_text().strip() if STATE_FILE.exists() else None
    if sha == last:
        return  # nada novo — não loga, pra não encher o log a cada ciclo do cron

    log(f"Commit novo em main: {sha[:10]} (último deployado: {(last or 'nenhum')[:10]})")

    if not ci_passed(sha):
        return

    dry = sync(apply=False)
    log("Dry-run do sync (o que vai mudar):\n" + (dry.stdout or "") + (dry.stderr or ""))

    real = sync(apply=True)
    log("Saída do sync --apply:\n" + (real.stdout or "") + (real.stderr or ""))
    if real.returncode != 0:
        log(f"sync-from-git.sh --apply falhou (exit={real.returncode}) — NÃO marcando {sha[:10]} como deployado")
        return

    post_deploy_healthcheck()
    STATE_FILE.write_text(sha)
    log(f"Deploy concluído e registrado: {sha[:10]}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # nunca deixa o cron morrer silenciosamente
        log(f"ERRO inesperado em auto-sync.py: {exc!r}")
        sys.exit(1)
