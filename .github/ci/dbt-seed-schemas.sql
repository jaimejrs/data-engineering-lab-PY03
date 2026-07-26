-- Passo 1/2 do seed do CI (job dbt-integration): só os CREATE SCHEMA.
-- Separado de dbt-seed-tables.sql porque quem cria o diretório do schema
-- (<warehouse>/<schema>.db) é o Hive Metastore, não o Trino — e o HMS roda
-- como usuário diferente dentro do próprio container. Isso significa que um
-- `chmod -R 777 /warehouse` rodado ANTES do CREATE SCHEMA não alcança esse
-- diretório (ele ainda não existe); é preciso rodar o chmod de novo DEPOIS
-- do schema criado e ANTES de criar as tabelas dentro dele (achado durante
-- a 1ª execução real do job em CI: mesmo erro de permissão já corrigido
-- manualmente no servidor voltou a acontecer, porque lá o chmod tinha sido
-- feito depois dos schemas já existirem).

CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 'file:///warehouse/silver.db');
CREATE SCHEMA IF NOT EXISTS iceberg.gold WITH (location = 'file:///warehouse/gold.db');
CREATE SCHEMA IF NOT EXISTS iceberg.ml WITH (location = 'file:///warehouse/ml.db');
CREATE SCHEMA IF NOT EXISTS iceberg.audit WITH (location = 'file:///warehouse/audit.db');
