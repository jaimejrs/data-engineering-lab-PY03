-- Colunas de empenhos usadas pela Gold. Chave de negócio (id, ano) — ver
-- README/dicionario-dados: `id` sozinho se repete entre anos.
select
    id,
    ano,
    dataemissao,
    codigoug,
    codprocesso,
    valor,
    modalidade
from {{ source('silver', 'empenhos') }}
