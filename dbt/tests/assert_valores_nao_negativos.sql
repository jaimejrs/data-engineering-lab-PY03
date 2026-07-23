-- Teste singular: nenhum valor monetário negativo nos fatos.
-- dbt falha se esta query retornar linhas.
select 'fato_contrato' AS tabela, 'valor_contrato' AS coluna, valor_contrato AS valor
from {{ ref('fato_contrato') }}
where valor_contrato < 0

union all
select 'fato_contrato', 'valor_pago', valor_pago
from {{ ref('fato_contrato') }}
where valor_pago < 0

union all
select 'fato_empenho', 'valor', valor
from {{ ref('fato_empenho') }}
where valor < 0
