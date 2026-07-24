-- Reconciliação Silver -> Gold e cobertura de join dos fatos (itens 4 e 6 de
-- docs/06-analise-critica.md). Antes disso, um `ano` não-parseável em
-- `fato_contrato` ou uma FK sem match viravam perda silenciosa — só visível
-- lendo o código. Agora é uma tabela consultável, com teste WARN em
-- `dbt/tests/assert_cobertura_gold_minima.sql` quando algum percentual foge
-- do esperado.
{{ config(materialized='table') }}

with silver_contratos as (
    select count(*) as total from {{ source('silver', 'contratos') }} where id is not null
),
silver_empenhos as (
    select count(*) as total from {{ source('silver', 'empenhos') }} where id is not null and ano is not null
),
silver_ob as (
    select count(*) as total
    from {{ source('silver', 'ordem_bancaria_orcamentaria') }}
    where id is not null and ano is not null
),
contrato as (
    select
        count(*) as total_linhas,
        count(*) filter (where sk_orgao is null) as sem_orgao,
        count(*) filter (where sk_credor is null) as sem_credor,
        count(*) filter (where sk_modalidade is null) as sem_modalidade,
        count(*) filter (where sk_tempo is null) as sem_tempo
    from {{ ref('fato_contrato') }}
),
empenho as (
    select
        count(*) as total_linhas,
        count(*) filter (where sk_orgao is null) as sem_orgao,
        count(*) filter (where sk_tempo is null) as sem_tempo
    from {{ ref('fato_empenho') }}
),
ob as (
    select
        count(*) as total_linhas,
        count(*) filter (where sk_orgao is null) as sem_orgao,
        count(*) filter (where sk_tempo is null) as sem_tempo
    from {{ ref('fato_ordem_bancaria') }}
)
select
    'fato_contrato' as tabela,
    (select total from silver_contratos) as linhas_origem_silver,
    contrato.total_linhas as linhas_gold,
    (select total from silver_contratos) - contrato.total_linhas as linhas_descartadas,
    round(
        100.0 * ((select total from silver_contratos) - contrato.total_linhas)
        / nullif((select total from silver_contratos), 0),
    3) as pct_descartadas,
    contrato.sem_orgao,
    round(100.0 * contrato.sem_orgao / nullif(contrato.total_linhas, 0), 3) as pct_sem_orgao,
    contrato.sem_credor,
    round(100.0 * contrato.sem_credor / nullif(contrato.total_linhas, 0), 3) as pct_sem_credor,
    contrato.sem_modalidade,
    round(100.0 * contrato.sem_modalidade / nullif(contrato.total_linhas, 0), 3) as pct_sem_modalidade,
    contrato.sem_tempo,
    round(100.0 * contrato.sem_tempo / nullif(contrato.total_linhas, 0), 3) as pct_sem_tempo,
    current_timestamp as gerado_em
from contrato

union all

select
    'fato_empenho' as tabela,
    (select total from silver_empenhos) as linhas_origem_silver,
    empenho.total_linhas as linhas_gold,
    (select total from silver_empenhos) - empenho.total_linhas as linhas_descartadas,
    round(
        100.0 * ((select total from silver_empenhos) - empenho.total_linhas)
        / nullif((select total from silver_empenhos), 0),
    3) as pct_descartadas,
    empenho.sem_orgao,
    round(100.0 * empenho.sem_orgao / nullif(empenho.total_linhas, 0), 3) as pct_sem_orgao,
    cast(null as bigint) as sem_credor,
    cast(null as double) as pct_sem_credor,
    cast(null as bigint) as sem_modalidade,
    cast(null as double) as pct_sem_modalidade,
    empenho.sem_tempo,
    round(100.0 * empenho.sem_tempo / nullif(empenho.total_linhas, 0), 3) as pct_sem_tempo,
    current_timestamp as gerado_em
from empenho

union all

select
    'fato_ordem_bancaria' as tabela,
    (select total from silver_ob) as linhas_origem_silver,
    ob.total_linhas as linhas_gold,
    (select total from silver_ob) - ob.total_linhas as linhas_descartadas,
    round(
        100.0 * ((select total from silver_ob) - ob.total_linhas)
        / nullif((select total from silver_ob), 0),
    3) as pct_descartadas,
    ob.sem_orgao,
    round(100.0 * ob.sem_orgao / nullif(ob.total_linhas, 0), 3) as pct_sem_orgao,
    cast(null as bigint) as sem_credor,
    cast(null as double) as pct_sem_credor,
    cast(null as bigint) as sem_modalidade,
    cast(null as double) as pct_sem_modalidade,
    ob.sem_tempo,
    round(100.0 * ob.sem_tempo / nullif(ob.total_linhas, 0), 3) as pct_sem_tempo,
    current_timestamp as gerado_em
from ob
