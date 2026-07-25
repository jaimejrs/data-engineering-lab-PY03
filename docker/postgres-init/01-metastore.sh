#!/bin/bash
# Provisiona o backing store do Hive Metastore no Postgres de metadados.
#
# Era um .sql com usuário/senha hardcoded ('hive'/'hive') — virou .sh pra usar
# METASTORE_DB_USER/METASTORE_DB_PASSWORD (env do container, default 'hive'
# só pro stack local descartável — ver docker-compose.yml/.env.example).
#
# ATENÇÃO: scripts em /docker-entrypoint-initdb.d só rodam quando o volume do
# Postgres é criado do zero. Se o volume `postgres_data` já existe (pipeline já
# rodou antes), este script NÃO roda — crie o DB/usuário manualmente:
#
#   docker exec -it datalab_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
#     -c "CREATE USER ${METASTORE_DB_USER:-hive} WITH PASSWORD '${METASTORE_DB_PASSWORD:-hive}';" \
#     -c "CREATE DATABASE metastore OWNER ${METASTORE_DB_USER:-hive};"
#
# (ver documentacao/lakehouse-spark-iceberg.md, seção de runbook).

set -e

METASTORE_DB_USER="${METASTORE_DB_USER:-hive}"
METASTORE_DB_PASSWORD="${METASTORE_DB_PASSWORD:-hive}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${METASTORE_DB_USER} WITH PASSWORD '${METASTORE_DB_PASSWORD}';
    CREATE DATABASE metastore OWNER ${METASTORE_DB_USER};
    GRANT ALL PRIVILEGES ON DATABASE metastore TO ${METASTORE_DB_USER};
EOSQL
