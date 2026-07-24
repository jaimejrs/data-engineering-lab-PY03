-- fato_ordem_bancaria — 3º estágio da despesa (contrato -> empenho -> ordem
-- bancária = pagamento efetivo ao credor). Item 3 de docs/06-analise-critica.md:
-- a fonte já era ingerida até a Silver (1,4M linhas), mas não tinha modelo Gold.
-- Chave de negócio (id_ob_origem, ano) — mesma lógica de fato_empenho (id
-- sozinho se repete entre anos, sem PK real na origem). Particionada por `ano`.
{{ config(materialized='table', properties={'partitioning': "ARRAY['ano']"}) }}

with o as (
    select
        *,
        row_number() over (partition by id, ano order by dataemissao desc) as rn
    from {{ ref('stg_ordem_bancaria') }}
    where id is not null and ano is not null
),
ordens as (
    select * from o where rn = 1
)
select
    cast(ordens.id as bigint) as id_ob_origem,
    cast(ordens.ano as integer) as ano,
    ordens.codigo as codigo_ob,
    dorg.sk_orgao,
    dtmp.sk_tempo,
    ordens.codprocesso as cod_contrato,
    try_cast(ordens.valor as decimal(15, 2)) as valor,
    ordens.codnatureza as natureza,
    ordens.statusdocumento as status,
    (ordens.datacancelamento is not null) as flag_cancelada
from ordens
left join {{ ref('dim_orgao') }} dorg
    on dorg.codigo = cast(ordens.codigoug as varchar)
    and dorg.ano = cast(ordens.ano as integer)
left join {{ ref('dim_tempo') }} dtmp
    on dtmp.data = try(cast(substr(ordens.dataemissao, 1, 10) as date))
