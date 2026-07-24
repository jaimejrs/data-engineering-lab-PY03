-- dim_tempo — união das datas de contratos (data_assinatura) e empenhos
-- (dataemissao), já ISO na Silver. `try(cast ... as date)` descarta valores não
-- normalizados (ex: as ~120k datas ISO-com-timezone documentadas no dicionário).
-- substr(...,1,10) cobre datas ISO com timezone (ex: contratos
-- '2026-07-21T00:00:00.000-03:00'), ISO puro e o que a Silver já normalizou.
with datas as (
    select try(cast(substr(data_assinatura, 1, 10) as date)) as data from {{ ref('stg_contratos') }}
    union
    select try(cast(substr(dataemissao, 1, 10) as date)) from {{ ref('stg_empenhos') }}
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
