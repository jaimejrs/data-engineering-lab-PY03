-- Teste singular: chave de negócio composta única nas fontes Silver — nenhuma
-- delas tem PK real na origem (ver README/dicionario-dados), então a garantia
-- de unicidade vem só do MERGE INTO por chave (src/spark_jobs/silver_job.py,
-- DEDUP_KEYS). Nada testava isso até 26/07/2026 — uma regressão no dedup não
-- seria pega. dbt falha se esta query retornar linhas.
select 'empenhos' as tabela, cast(id as varchar) as chave, ano, count(*) as linhas
from {{ source('silver', 'empenhos') }}
group by id, ano
having count(*) > 1

union all
select 'ordem_bancaria_orcamentaria', cast(id as varchar), ano, count(*)
from {{ source('silver', 'ordem_bancaria_orcamentaria') }}
group by id, ano
having count(*) > 1

union all
select 'unidade_gestora', codigo, ano, count(*)
from {{ source('silver', 'unidade_gestora') }}
group by codigo, ano
having count(*) > 1
