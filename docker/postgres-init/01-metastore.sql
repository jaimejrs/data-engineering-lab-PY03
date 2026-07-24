-- Provisiona o backing store do Hive Metastore no Postgres de metadados.
--
-- ATENÇÃO: scripts em /docker-entrypoint-initdb.d só rodam quando o volume do
-- Postgres é criado do zero. Se o volume `postgres_data` já existe (pipeline já
-- rodou antes), este script NÃO roda — crie o DB/usuário manualmente:
--
--   docker exec -it datalab_postgres psql -U dlab -d datalab \
--     -c "CREATE USER hive WITH PASSWORD 'hive';" \
--     -c "CREATE DATABASE metastore OWNER hive;"
--
-- (ver documentacao/lakehouse-spark-iceberg.md, seção de runbook).

CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE metastore OWNER hive;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
