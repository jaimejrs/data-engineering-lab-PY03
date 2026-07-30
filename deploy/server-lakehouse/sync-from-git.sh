#!/usr/bin/env bash
# Deploy via `git pull` + `rsync` local, no lugar de `scp`/SFTP arquivo a
# arquivo (item 12 de docs/06-analise-critica.md). Só ficou possível depois
# do fix de egress IPv4 (deploy/server-lakehouse/FIX-EGRESS-IPV4.md) — antes
# disso o servidor não alcançava o GitHub, só IPv6.
#
# Uso (no servidor, usuário dataadm):
#   ./sync-from-git.sh            # dry-run — só mostra o que mudaria
#   ./sync-from-git.sh --apply    # aplica de verdade
#
# O que faz:
#   1. `git pull` num clone read-only em `~/repo` (clona na 1a vez se não existir).
#   2. `rsync` dos subdiretórios relevantes pros 2 diretórios de deploy live
#      (`~/lakehouse/` e `~/airflow/`) — nunca toca no resto desses
#      diretórios (artefatos gerados: `models/artifacts/*.joblib`, `logs/`,
#      `mlruns/`, os `docker-compose*.yml`/overlays com config específica do
#      servidor, que não vivem no repo).
#
# Sem --apply, roda `rsync -n` (dry-run) por padrão — a primeira vez que
# qualquer script mexe nesses diretórios via rsync (histórico do projeto é só
# scp arquivo a arquivo), vale conferir o que ele PROPÕE trocar antes de
# aplicar de verdade, especialmente por causa do --delete (remove no destino
# o que não existe mais no repo).
set -euo pipefail

if ! command -v rsync >/dev/null 2>&1; then
  echo "ERRO: rsync não está instalado neste servidor (sudo apt install rsync)." >&2
  echo "Sem rsync, o script para aqui de propósito — não faz sync com cp/rm -rf" >&2
  echo "num diretório live (bind-mount de containers rodando), é bem mais arriscado" >&2
  echo "que o --delete do rsync (sem janela onde o destino fica parcialmente vazio)." >&2
  exit 1
fi

REPO_DIR="${REPO_DIR:-$HOME/repo}"
REPO_URL="${REPO_URL:-https://github.com/jaimejrs/data-engineering-lab-PY03}"
LAKEHOUSE_DIR="${LAKEHOUSE_DIR:-$HOME/lakehouse}"
AIRFLOW_DIR="${AIRFLOW_DIR:-$HOME/airflow}"

DRY_RUN_FLAG="-n"
if [ "${1:-}" = "--apply" ]; then
  DRY_RUN_FLAG=""
  echo ">> Modo APLICAR — vai escrever de verdade nos diretórios live."
else
  echo ">> Modo DRY-RUN (padrão) — nada será escrito. Rode com --apply para aplicar."
fi

if [ ! -d "$REPO_DIR/.git" ]; then
  echo ">> Clonando $REPO_URL em $REPO_DIR (1a vez)"
  git clone "$REPO_URL" "$REPO_DIR"
else
  echo ">> git pull em $REPO_DIR"
  git -C "$REPO_DIR" pull --ff-only
fi

echo ">> lakehouse/ (dbt, src — target/ e logs/ preservados, são gerados pelo container dbt e não vêm do repo)"
rsync -av $DRY_RUN_FLAG --delete --exclude 'target/' --exclude 'logs/' --exclude '.user.yml' "$REPO_DIR/dbt/" "$LAKEHOUSE_DIR/dbt/"
rsync -av $DRY_RUN_FLAG --delete --exclude '__pycache__/' "$REPO_DIR/src/" "$LAKEHOUSE_DIR/src/"
# --exclude '.env': o real nunca está no clone read-only ($REPO_DIR, nunca
# commitado), mas protege um .env colocado manualmente em $LAKEHOUSE_DIR/streamlit/
# (override local) de ser apagado pelo --delete numa sincronização futura.
rsync -av $DRY_RUN_FLAG --delete --exclude '.env' --exclude '__pycache__/' "$REPO_DIR/streamlit/" "$LAKEHOUSE_DIR/streamlit/"

echo ">> airflow/ (dags, models, src — artifacts/ e __pycache__/ preservados, não vêm do repo)"
rsync -av $DRY_RUN_FLAG --delete --exclude '__pycache__/' "$REPO_DIR/dags/" "$AIRFLOW_DIR/dags/"
rsync -av $DRY_RUN_FLAG --delete --exclude 'artifacts/' --exclude '__pycache__/' "$REPO_DIR/models/" "$AIRFLOW_DIR/models/"
rsync -av $DRY_RUN_FLAG --delete --exclude '__pycache__/' "$REPO_DIR/src/" "$AIRFLOW_DIR/src/"

echo "OK. Se algum requirements.txt mudou, ainda é preciso rebuildar a imagem"
echo "Docker correspondente na mão — este script só sincroniza código-fonte."
