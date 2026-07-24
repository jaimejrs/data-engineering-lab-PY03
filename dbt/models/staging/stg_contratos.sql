-- Colunas de contratos usadas pela Gold (ver src/loaders/dw_loader.py como
-- fonte da regra de negócio). Silver já normalizou datas e cnpj_cpf/tipo_credor.
select
    id,
    cnpj_cpf_normalizado,
    tipo_credor,
    descricao_nome_credor,
    infringement_status,
    descricao_modalidade,
    data_assinatura,
    cod_gestora,
    valor_contrato,
    calculated_valor_pago,
    calculated_valor_empenhado,
    descricao_situacao,
    emergency
from {{ source('silver', 'contratos') }}
