-- Teste singular WARN: a Silver deduplicada nunca deveria ter MAIS linhas do
-- que a soma de tudo que a Bronze já validou (o MERGE INTO só remove
-- duplicata, nunca cria linha do nada) — se acontecer, é sinal de duplicação
-- real na Silver, não de reconciliação "quase batendo". Ver
-- dbt/models/marts/bronze_silver_reconciliacao.sql e docs/06-analise-critica.md
-- (item 6).
{{ config(severity='warn') }}
select *
from {{ ref('bronze_silver_reconciliacao') }}
where total_silver > total_bronze
