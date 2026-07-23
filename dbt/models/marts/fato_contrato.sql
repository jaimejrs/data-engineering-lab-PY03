-- fato_contrato — porta de dw_loader.load_fato_contrato. SK vêm por left join às
-- dimensões (null quando não há match — mesma semântica dos maps do pandas).
-- Chave de negócio (id_contrato_origem, ano). Particionada por `ano` (Iceberg).
{{ config(materialized='table', properties={'partitioning': "ARRAY['ano']"}) }}

with c as (
    select
        *,
        year(try(cast(substr(data_assinatura, 1, 10) as date))) as ano_calc,
        row_number() over (partition by id order by data_assinatura desc) as rn
    from {{ ref('stg_contratos') }}
    where id is not null
),
contratos as (
    select * from c where rn = 1 and ano_calc is not null
)
select
    cast(contratos.ano_calc as integer) as ano,
    cast(contratos.id as varchar) as id_contrato_origem,
    dc.sk_credor,
    dorg.sk_orgao,
    dmod.sk_modalidade,
    dtmp.sk_tempo,
    try_cast(contratos.valor_contrato as decimal(15, 2)) as valor_contrato,
    try_cast(contratos.calculated_valor_pago as decimal(15, 2)) as valor_pago,
    try_cast(contratos.calculated_valor_empenhado as decimal(15, 2)) as valor_empenhado,
    contratos.descricao_situacao as status,
    coalesce(try_cast(contratos.emergency as boolean), false) as flag_emergency,
    cast(null as decimal(5, 4)) as score_anomalia
from contratos
left join {{ ref('dim_credor') }} dc
    on dc.cnpj_cpf = contratos.cnpj_cpf_normalizado
left join {{ ref('dim_orgao') }} dorg
    on dorg.codigo = cast(contratos.cod_gestora as varchar)
    and dorg.ano = cast(contratos.ano_calc as integer)
left join {{ ref('dim_modalidade') }} dmod
    on dmod.descricao_modalidade = contratos.descricao_modalidade
left join {{ ref('dim_tempo') }} dtmp
    on dtmp.data = try(cast(substr(contratos.data_assinatura, 1, 10) as date))
