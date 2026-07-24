-- fato_empenho — porta de dw_loader.load_fato_empenho. Chave de negócio
-- (id_empenho_origem, ano). sk_orgao via (codigoug, ano) -> dim_orgao (mesmo join
-- lógico que o enriquecimento com unidade_gestora); sk_tempo via dataemissao.
-- Particionada por `ano` (Iceberg).
{{ config(materialized='table', properties={'partitioning': "ARRAY['ano']"}) }}

with e as (
    select
        *,
        row_number() over (partition by id, ano order by dataemissao desc) as rn
    from {{ ref('stg_empenhos') }}
    where id is not null and ano is not null
),
empenhos as (
    select * from e where rn = 1
)
select
    cast(empenhos.id as bigint) as id_empenho_origem,
    cast(empenhos.ano as integer) as ano,
    dorg.sk_orgao,
    dtmp.sk_tempo,
    empenhos.codprocesso as cod_contrato,
    try_cast(empenhos.valor as decimal(15, 2)) as valor,
    empenhos.modalidade
from empenhos
left join {{ ref('dim_orgao') }} dorg
    on dorg.codigo = cast(empenhos.codigoug as varchar)
    and dorg.ano = cast(empenhos.ano as integer)
left join {{ ref('dim_tempo') }} dtmp
    on dtmp.data = try(cast(substr(empenhos.dataemissao, 1, 10) as date))
