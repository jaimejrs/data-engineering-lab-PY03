-- dim_modalidade — distinct de descricao_modalidade (não-vazio).
with base as (
    select distinct descricao_modalidade
    from {{ ref('stg_contratos') }}
    where descricao_modalidade is not null and descricao_modalidade <> ''
)
select
    {{ sk(['descricao_modalidade']) }} as sk_modalidade,
    descricao_modalidade
from base
