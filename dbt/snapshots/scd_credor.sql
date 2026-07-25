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
-- BUG corrigido (25/07/2026, achado revisando os scripts da Fernanda contra a
-- Gold real): o dedup por credor escolhia UMA linha via row_number()/nome
-- (ordem alfabética) e usava o historico_infringement DAQUELA linha só. Um
-- credor com vários contratos e só UM deles com infringement_status > 0 tinha
-- ~1/N de chance de sobreviver o dedup — na prática, o único registro real da
-- base (iceberg.silver.contratos) não sobrevivia, e dim_credor.
-- historico_infringement saía 100% False pra todo mundo (confirmado: fonte
-- tem 1 registro com infringement_status > 0, Gold tinha 0 credores
-- marcados). historico_infringement agora agrega com bool_or por TODOS os
-- contratos do credor — "esse credor já teve alguma infração registrada em
-- qualquer contrato", não "o que a linha escolhida por nome diz".
with base as (
    select
        cnpj_cpf_normalizado as cnpj_cpf,
        descricao_nome_credor as nome,
        tipo_credor as tipo,
        coalesce(try_cast(infringement_status as double), 0) > 0 as infringement_registro,
        row_number() over (partition by cnpj_cpf_normalizado order by descricao_nome_credor) as rn
    from {{ source('silver', 'contratos') }}
    where cnpj_cpf_normalizado is not null and cnpj_cpf_normalizado <> ''
),
historico_por_credor as (
    select cnpj_cpf, bool_or(infringement_registro) as historico_infringement
    from base
    group by cnpj_cpf
)
select b.cnpj_cpf, b.nome, b.tipo, h.historico_infringement
from base b
join historico_por_credor h on h.cnpj_cpf = b.cnpj_cpf
where b.rn = 1
{% endsnapshot %}
