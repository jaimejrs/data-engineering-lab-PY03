-- dim_orgao — uma linha por (codigo, ano). Renomes conforme dw_loader.load_dim_orgao.
with base as (
    select
        cast(codigo as varchar) as codigo,
        cast(ano as integer) as ano,
        titulo as nome,
        sigla,
        cnpj,
        tipoadministracao as tipo_administracao,
        tipoug as tipo_ug,
        codigopoder as codigo_poder,
        nomepoder as nome_poder,
        codigouf as codigo_uf,
        nomemunicipio as nome_municipio,
        row_number() over (partition by codigo, ano order by titulo) as rn
    from {{ ref('stg_unidade_gestora') }}
    where codigo is not null and ano is not null
)
select
    {{ sk(['codigo', 'ano']) }} as sk_orgao,
    codigo,
    ano,
    nome,
    sigla,
    cnpj,
    tipo_administracao,
    tipo_ug,
    codigo_poder,
    nome_poder,
    codigo_uf,
    nome_municipio
from base
where rn = 1
