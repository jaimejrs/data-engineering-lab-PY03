-- dim_tempo — união das datas de contratos (data_assinatura) e empenhos
-- (dataemissao), já ISO na Silver. `try(cast ... as date)` descarta valores não
-- normalizados (ex: as ~120k datas ISO-com-timezone documentadas no dicionário).
with datas as (
    select try(cast(data_assinatura as date)) as data from {{ ref('stg_contratos') }}
    union
    select try(cast(dataemissao as date)) from {{ ref('stg_empenhos') }}
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
