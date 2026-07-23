#!/bin/bash
# Entrypoint do Hive Metastore: inicializa o schema no Postgres de forma
# idempotente (só quando ainda não existe) e sobe o serviço metastore em foreground.
set -euo pipefail

echo "[metastore] Verificando schema no Postgres (DB metastore)..."
if ! schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "[metastore] Schema ausente — inicializando (schematool -initSchema)..."
  schematool -dbType postgres -initSchema
else
  echo "[metastore] Schema já inicializado — seguindo."
fi

echo "[metastore] Iniciando Hive Metastore (thrift :9083)..."
exec hive --service metastore
