-- dim_tempo — união das datas de contratos (data_assinatura), empenhos
-- (dataemissao) e ordens bancárias (dataemissao). Já vêm como DATE desde a
-- Silver (tipagem aplicada em 26/07/2026 — antes vinha como varchar, exigia
-- try(cast(substr(...))) aqui; ver stacks/apache-iceberg.md).
with datas as (
    select data_assinatura as data from {{ ref('stg_contratos') }}
    union
    select dataemissao from {{ ref('stg_empenhos') }}
    union
    select dataemissao from {{ ref('stg_ordem_bancaria') }}
),
validas as (
    select distinct data from datas where data is not null
)
select
    {{ sk(['data']) }} as sk_tempo,
    data,
    year(data) as ano,
    quarter(data) as trimestre,
    month(data) as mes,
    day_of_week(data) as dia_semana
from validas
