-- Reconciliação Bronze -> Silver: soma as contagens que a validação da
-- Bronze já persistiu por execução (`iceberg.audit.bronze_ingestao`, via
-- src/reconciliation.py) e compara com a contagem ao vivo da Silver — mais
-- barato do que reler o JSON da Bronze inteira.
--
-- Só fontes INCREMENTAIS (empenhos, ordem_bancaria_orcamentaria, contratos):
-- `unidade_gestora` é recarregada por inteiro a cada execução, então somar
-- contagens diárias infla sem limite (completude coberta à parte por
-- DEFAULT_MIN_RECORDS_BY_SOURCE em bronze_validator.py).
--
-- Não é reconciliação byte-a-byte: janelas incrementais sobrepostas (lookback
-- do watermark) fazem o Bronze somado ficar acima da Silver deduplicada por
-- natureza — esperado, não é teste de "bateram exato". O teste WARN
-- (assert_reconciliacao_bronze_silver.sql) verifica só o caso anômalo:
-- Silver maior que tudo que a Bronze já validou.
{{ config(materialized='table', schema='audit') }}

with bronze as (
    select fonte, sum(registros) as total_bronze
    from {{ source('audit', 'bronze_ingestao') }}
    where fonte in ('empenhos', 'ordem_bancaria_orcamentaria', 'contratos')
    group by fonte
),
silver_empenhos as (
    select 'empenhos' as fonte, count(*) as total_silver from {{ source('silver', 'empenhos') }}
),
silver_ob as (
    select 'ordem_bancaria_orcamentaria' as fonte, count(*) as total_silver
    from {{ source('silver', 'ordem_bancaria_orcamentaria') }}
),
silver_contratos as (
    select 'contratos' as fonte, count(*) as total_silver from {{ source('silver', 'contratos') }}
),
silver as (
    select * from silver_empenhos
    union all select * from silver_ob
    union all select * from silver_contratos
)
select
    coalesce(bronze.fonte, silver.fonte) as fonte,
    bronze.total_bronze,
    silver.total_silver,
    round(100.0 * silver.total_silver / nullif(bronze.total_bronze, 0), 2) as pct_silver_sobre_bronze,
    current_timestamp as gerado_em
from bronze
full outer join silver on bronze.fonte = silver.fonte
