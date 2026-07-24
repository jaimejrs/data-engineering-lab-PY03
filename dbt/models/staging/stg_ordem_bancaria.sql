-- Colunas de ordem_bancaria_orcamentaria usadas pela Gold. Silver já
-- normalizou dataemissao para ISO. Schema confirmado contra dado real no HDFS
-- (WebHDFS, 24/07/2026) — ver docs/06-analise-critica.md, item 3.
select
    id,
    ano,
    codigo,
    codigoug,
    dataemissao,
    datacancelamento,
    valor,
    codnatureza,
    statusdocumento,
    codprocesso
from {{ source('silver', 'ordem_bancaria_orcamentaria') }}
