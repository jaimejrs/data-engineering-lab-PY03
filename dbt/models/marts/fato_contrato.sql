-- fato_contrato — porta de dw_loader.load_fato_contrato. SK vêm por left join às
-- dimensões (null quando não há match — mesma semântica dos maps do pandas).
-- Chave de negócio (id_contrato_origem, ano). Particionada por `ano` (Iceberg).
{{ config(materialized='table', properties={'partitioning': "ARRAY['ano']"}) }}

with c as (
    select
        *,
        year(try(cast(substr(data_assinatura, 1, 10) as date))) as ano_calc,
        try(cast(substr(data_assinatura, 1, 10) as date)) as data_assinatura_date,
        row_number() over (partition by id order by data_assinatura desc) as rn
    from {{ ref('stg_contratos') }}
    where id is not null
),
contratos as (
    select * from c where rn = 1 and ano_calc is not null
),
-- SCD2 point-in-time (item 5 de docs/06-analise-critica.md): junta a versão de
-- dim_credor VIGENTE na data_assinatura do contrato, não a `versao_atual`.
-- Quando nenhuma versão cobre a data (comum pra contratos assinados antes do
-- snapshot dbt começar a acumular histórico — a 1ª carga registra só o estado
-- "hoje" de cada credor), cai pra versão mais antiga conhecida (menor
-- `valido_de`) em vez da atual — melhor aproximação histórica do que mostrar o
-- nome de hoje num contrato antigo.
credor_pit as (
    select
        contratos.id as contrato_id,
        dc.sk_credor,
        row_number() over (
            partition by contratos.id
            order by
                case
                    when contratos.data_assinatura_date >= cast(dc.valido_de as date)
                        and (dc.valido_ate is null or contratos.data_assinatura_date < cast(dc.valido_ate as date))
                    then 0
                    else 1
                end,
                dc.valido_de asc
        ) as rn
    from contratos
    left join {{ ref('dim_credor') }} dc
        on dc.cnpj_cpf = contratos.cnpj_cpf_normalizado
)
select
    cast(contratos.ano_calc as integer) as ano,
    cast(contratos.id as varchar) as id_contrato_origem,
    -- Chave p/ ligar (best-effort, ~7-8% de match real — não é FK obrigatória)
    -- com `cod_contrato` de fato_empenho/fato_ordem_bancaria. Antes só existia
    -- na Silver; não era propagada até aqui, então esse join não era possível
    -- na Gold (achado da análise crítica de 26/07/2026).
    contratos.num_spu,
    cp.sk_credor,
    dorg.sk_orgao,
    dmod.sk_modalidade,
    dtmp.sk_tempo,
    try_cast(contratos.valor_contrato as decimal(15, 2)) as valor_contrato,
    try_cast(contratos.calculated_valor_pago as decimal(15, 2)) as valor_pago,
    try_cast(contratos.calculated_valor_empenhado as decimal(15, 2)) as valor_empenhado,
    contratos.descricao_situacao as status,
    coalesce(try_cast(contratos.emergency as boolean), false) as flag_emergency,
    cast(sa.score_anomalia as decimal(5, 4)) as score_anomalia
from contratos
left join credor_pit cp
    on cp.contrato_id = contratos.id and cp.rn = 1
left join {{ ref('dim_orgao') }} dorg
    on dorg.codigo = cast(contratos.cod_gestora as varchar)
    and dorg.ano = cast(contratos.ano_calc as integer)
left join {{ ref('dim_modalidade') }} dmod
    on dmod.descricao_modalidade = contratos.descricao_modalidade
left join {{ ref('dim_tempo') }} dtmp
    on dtmp.data = contratos.data_assinatura_date
-- Modelo 1 (models/anomaly_detection.py) escreve aqui, fora do dbt — ver
-- sources.yml (`ml_scores`) para o motivo de não fazer UPDATE direto no fato.
left join {{ source('ml_scores', 'score_anomalia_contrato') }} sa
    on sa.id_contrato_origem = cast(contratos.id as varchar)
    and sa.ano = cast(contratos.ano_calc as integer)
