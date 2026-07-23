-- dim_credor — Type-1 (uma linha por cnpj_cpf), fiel ao comportamento atual do
-- dw_loader (que só mantém a versão "atual", nunca abre versão nova). SCD2 real
-- (dbt snapshot) fica como melhoria futura.
with base as (
    select
        cnpj_cpf_normalizado as cnpj_cpf,
        descricao_nome_credor as nome,
        tipo_credor as tipo,
        coalesce(try_cast(infringement_status as double), 0) > 0 as historico_infringement,
        row_number() over (
            partition by cnpj_cpf_normalizado
            order by descricao_nome_credor
        ) as rn
    from {{ ref('stg_contratos') }}
    where cnpj_cpf_normalizado is not null and cnpj_cpf_normalizado <> ''
)
select
    {{ sk(['cnpj_cpf']) }} as sk_credor,
    cnpj_cpf,
    nome,
    tipo,
    historico_infringement
from base
where rn = 1
