-- dim_credor — SCD2, materializada a partir do snapshot `scd_credor`.
-- A surrogate key é por VERSÃO (inclui o início de validade) para ser única
-- mesmo com múltiplas versões do mesmo cnpj_cpf. Fatos referenciam a versão
-- vigente (ver join `versao_atual` em fato_contrato).
select
    {{ sk(['cnpj_cpf', 'dbt_valid_from']) }} as sk_credor,
    cnpj_cpf,
    nome,
    tipo,
    historico_infringement,
    dbt_valid_from as valido_de,
    dbt_valid_to   as valido_ate,
    (dbt_valid_to is null) as versao_atual
from {{ ref('scd_credor') }}
