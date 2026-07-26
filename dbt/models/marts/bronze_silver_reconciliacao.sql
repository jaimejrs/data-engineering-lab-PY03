-- Reconciliação Bronze -> Silver (item 6 de docs/06-analise-critica.md).
-- Bronze é JSON em arquivo; contar exato de novo custaria reler tudo (mesmo
-- custo que validate_bronze já paga, ver item 7). Em vez disso, soma as
-- contagens que a própria validação da Bronze já persistiu por execução
-- (`iceberg.gold.bronze_ingestao`, escrita por src/reconciliation.py na DAG 1)
-- e compara com a contagem ao vivo da Silver (Iceberg, barata via Trino).
--
-- Só as fontes INCREMENTAIS (empenhos, ordem_bancaria_orcamentaria, contratos)
-- entram aqui: `unidade_gestora` é recarregada por inteiro a cada execução
-- (sem filtro de data), então somar suas contagens diárias não faz sentido
-- (infla sem limite) — sua completude já é coberta por
-- `DEFAULT_MIN_RECORDS_BY_SOURCE` em bronze_validator.py.
--
-- Não é reconciliação byte-a-byte: janelas incrementais sobrepostas (lookback
-- do watermark) fazem o Bronze somado ficar ACIMA da Silver deduplicada por
-- natureza (MERGE INTO só reduz, nunca aumenta) — isso é esperado, não é um
-- teste de "bateram exato". O que o teste WARN (assert_reconciliacao_bronze_
-- silver.sql) verifica é o caso realmente anômalo: Silver MAIOR que a soma de
-- tudo que a Bronze já validou, ou Silver muito abaixo do que seria plausível.
-- Schema físico `audit` (não `gold`) desde 26/07/2026 — é telemetria do
-- próprio pipeline, não parte do modelo dimensional.
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
