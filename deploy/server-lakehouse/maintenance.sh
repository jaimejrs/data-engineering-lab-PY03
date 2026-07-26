#!/usr/bin/env bash
# Manutenção das tabelas Iceberg via Trino: compaction (optimize), expiração de
# snapshots antigos e remoção de arquivos órfãos. Execuções incrementais criam
# muitos arquivos pequenos — roda via cron diário (ver crontab do servidor).
# Achado real (26/07/2026): antes só existia como script manual, nunca
# agendado, e não cobria as tabelas ml/audit — justamente as que mais geram
# snapshot/arquivo pequeno pelo cron de 5 minutos.
#
# Achado real #2 (mesmo dia): `optimize` (compaction) na tabela inteira das
# maiores (silver.empenhos, ~1,37M linhas) derruba o Trino por pressão de
# memória de forma reproduzível (mem_limit: 6g do container, ver
# docker-compose) — mesmo padrão já visto na migração de tipo da Silver.
# Resolvido rodando `optimize` particionado por ano (WHERE ano = <ano>) nas
# tabelas grandes, em vez da tabela inteira de uma vez.
#
# Uso:  ./maintenance.sh            (retenção de snapshot default 7d)
#       RETENTION=30d ./maintenance.sh
set -uo pipefail

TRINO="${TRINO_CONTAINER:-lakehouse_trino}"
RET="${RETENTION:-7d}"
ANOS=(2022 2023 2024 2025 2026)

q() { docker exec "$TRINO" trino --execute "$1"; }

wait_healthy() {
  for _ in $(seq 1 20); do
    status=$(docker inspect "$TRINO" --format "{{.State.Health.Status}}" 2>/dev/null)
    [ "$status" = "healthy" ] && return 0
    sleep 5
  done
}

# Roda `$1` (uma instrução SQL), tolerando o Trino cair no meio (comum em
# operação pesada, ver achado #2 acima) — espera ficar saudável de novo e
# tenta mais 1x antes de desistir dessa instrução específica.
run_tolerante() {
  if ! q "$1"; then
    echo "   (falhou — aguardando Trino voltar e tentando de novo)"
    wait_healthy
    q "$1" || echo "   (falhou de novo — seguindo pra próxima)"
  fi
}

# Tabelas grandes/particionadas por ano — optimize fatiado por ano em vez da
# tabela inteira de uma vez (achado #2).
TABELAS_GRANDES=(
  iceberg.silver.empenhos
  iceberg.silver.ordem_bancaria_orcamentaria
  iceberg.silver.contratos
  iceberg.gold.fato_empenho
  iceberg.gold.fato_contrato
  iceberg.gold.fato_ordem_bancaria
)

# Tabelas pequenas — optimize na tabela inteira é seguro.
TABELAS_PEQUENAS=(
  iceberg.silver.unidade_gestora
  iceberg.gold.dim_credor
  iceberg.gold.dim_orgao
  iceberg.gold.dim_modalidade
  iceberg.gold.dim_tempo
  iceberg.gold.scd_credor
  iceberg.ml.score_anomalia_contrato
  iceberg.ml.previsao_pagamento_orgao
  iceberg.ml.relatorio_narrativo
  iceberg.audit.bronze_ingestao
  iceberg.audit.bronze_silver_reconciliacao
  iceberg.audit.gold_reconciliacao
  # As 4 abaixo são gravadas por cron a cada 5min (collect_infra_metrics.py,
  # collect_access_audit.py) — sem essa lista, acumulam snapshot/arquivo
  # pequeno sem limite (achado real: 28 arquivos de ~1-2KB cada depois de só
  # ~3h rodando).
  iceberg.audit.infra_metricas_containers
  iceberg.audit.infra_metricas_disco
  iceberg.audit.sessoes_ssh
  iceberg.audit.comandos_executados
)

for t in "${TABELAS_GRANDES[@]}"; do
  for ano in "${ANOS[@]}"; do
    echo ">> $t : optimize ano=$ano (compaction particionada)"
    run_tolerante "ALTER TABLE $t EXECUTE optimize WHERE ano = $ano"
  done
  echo ">> $t : expire_snapshots (retention=$RET)"
  run_tolerante "ALTER TABLE $t EXECUTE expire_snapshots(retention_threshold => '$RET')"
  echo ">> $t : remove_orphan_files (retention=$RET)"
  run_tolerante "ALTER TABLE $t EXECUTE remove_orphan_files(retention_threshold => '$RET')"
done

for t in "${TABELAS_PEQUENAS[@]}"; do
  echo ">> $t : optimize (compaction)"
  run_tolerante "ALTER TABLE $t EXECUTE optimize"
  echo ">> $t : expire_snapshots (retention=$RET)"
  run_tolerante "ALTER TABLE $t EXECUTE expire_snapshots(retention_threshold => '$RET')"
  echo ">> $t : remove_orphan_files (retention=$RET)"
  run_tolerante "ALTER TABLE $t EXECUTE remove_orphan_files(retention_threshold => '$RET')"
done

# Retenção de LINHA (não só de snapshot) nas tabelas de auditoria de acesso —
# guardam IP de origem e linha de comando completa (pode conter credencial
# digitada inline), dado sensível sem motivo pra reter indefinidamente.
# expire_snapshots acima não apaga linha nenhuma da tabela atual, só estado
# histórico — esse DELETE é o que efetivamente limita o quanto fica guardado.
echo ">> retenção de linha: sessoes_ssh/comandos_executados (${AUDIT_RETENTION_DAYS:-90} dias)"
run_tolerante "DELETE FROM iceberg.audit.sessoes_ssh WHERE timestamp_evento < current_timestamp - INTERVAL '${AUDIT_RETENTION_DAYS:-90}' DAY"
run_tolerante "DELETE FROM iceberg.audit.comandos_executados WHERE timestamp_evento < current_timestamp - INTERVAL '${AUDIT_RETENTION_DAYS:-90}' DAY"

echo ">> retenção de linha: infra_metricas_* (${METRICS_RETENTION_DAYS:-30} dias)"
run_tolerante "DELETE FROM iceberg.audit.infra_metricas_containers WHERE coletado_em < current_timestamp - INTERVAL '${METRICS_RETENTION_DAYS:-30}' DAY"
run_tolerante "DELETE FROM iceberg.audit.infra_metricas_disco WHERE coletado_em < current_timestamp - INTERVAL '${METRICS_RETENTION_DAYS:-30}' DAY"

echo "Manutenção concluída."
