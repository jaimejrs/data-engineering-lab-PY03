#!/bin/bash
# Provisiona o backing store de metadados do Superset no Postgres de metadados
# — mesmo padrão de 01-metastore.sh (usuário/DB dedicados, não hardcoded).
#
# ATENÇÃO: scripts em /docker-entrypoint-initdb.d só rodam quando o volume do
# Postgres é criado do zero. Se o volume já existe (caso do servidor real),
# crie o DB/usuário manualmente:
#
#   docker exec -it datalab_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
#     -c "CREATE USER ${SUPERSET_DB_USER:-superset} WITH PASSWORD '${SUPERSET_DB_PASSWORD:-superset}';" \
#     -c "CREATE DATABASE superset OWNER ${SUPERSET_DB_USER:-superset};"

set -e

SUPERSET_DB_USER="${SUPERSET_DB_USER:-superset}"
SUPERSET_DB_PASSWORD="${SUPERSET_DB_PASSWORD:-superset}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${SUPERSET_DB_USER} WITH PASSWORD '${SUPERSET_DB_PASSWORD}';
    CREATE DATABASE superset OWNER ${SUPERSET_DB_USER};
    GRANT ALL PRIVILEGES ON DATABASE superset TO ${SUPERSET_DB_USER};
EOSQL
