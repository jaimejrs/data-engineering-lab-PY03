{#
  SCD2 de credores. O snapshot historia mudanças de nome/tipo/infringement por
  cnpj_cpf ao longo das execuções: cada `dbt build`/`dbt snapshot` que detectar
  mudança (strategy=check) fecha a versão vigente (dbt_valid_to) e abre outra.
  Na 1ª execução todos entram como versão vigente (dbt_valid_to = NULL).
#}
{% snapshot scd_credor %}
{{
  config(
    target_schema='gold',
    unique_key='cnpj_cpf',
    strategy='check',
    check_cols=['nome', 'tipo', 'historico_infringement'],
    invalidate_hard_deletes=False,
  )
}}
with base as (
    select
        cnpj_cpf_normalizado as cnpj_cpf,
        descricao_nome_credor as nome,
        tipo_credor as tipo,
        coalesce(try_cast(infringement_status as double), 0) > 0 as historico_infringement,
        row_number() over (partition by cnpj_cpf_normalizado order by descricao_nome_credor) as rn
    from {{ source('silver', 'contratos') }}
    where cnpj_cpf_normalizado is not null and cnpj_cpf_normalizado <> ''
)
select cnpj_cpf, nome, tipo, historico_infringement
from base
where rn = 1
{% endsnapshot %}
