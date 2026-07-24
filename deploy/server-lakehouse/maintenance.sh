#!/usr/bin/env bash
# Manutenção das tabelas Iceberg via Trino: compaction (optimize), expiração de
# snapshots antigos e remoção de arquivos órfãos. Execuções incrementais criam
# muitos arquivos pequenos — rodar periodicamente (ex: cron semanal).
#
# Uso:  ./maintenance.sh            (retenção default 7d)
#       RETENTION=30d ./maintenance.sh
set -uo pipefail

TRINO="${TRINO_CONTAINER:-lakehouse_trino}"
RET="${RETENTION:-7d}"

TABLES=(
  iceberg.silver.empenhos
  iceberg.silver.ordem_bancaria_orcamentaria
  iceberg.silver.contratos
  iceberg.silver.unidade_gestora
  iceberg.gold.fato_empenho
  iceberg.gold.fato_contrato
  iceberg.gold.dim_credor
  iceberg.gold.dim_orgao
  iceberg.gold.dim_modalidade
  iceberg.gold.dim_tempo
)

q() { docker exec "$TRINO" trino --execute "$1"; }

for t in "${TABLES[@]}"; do
  echo ">> $t : optimize (compaction)"
  q "ALTER TABLE $t EXECUTE optimize" || echo "   (falhou/tabela ausente — seguindo)"
  echo ">> $t : expire_snapshots (retention=$RET)"
  q "ALTER TABLE $t EXECUTE expire_snapshots(retention_threshold => '$RET')" || true
  echo ">> $t : remove_orphan_files (retention=$RET)"
  q "ALTER TABLE $t EXECUTE remove_orphan_files(retention_threshold => '$RET')" || true
done
echo "Manutenção concluída."
