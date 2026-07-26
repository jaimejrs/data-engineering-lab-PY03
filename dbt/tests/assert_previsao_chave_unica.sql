-- Teste singular: chave de negócio única (codigo_orgao, ano_previsto,
-- trimestre_previsto) em previsao_pagamento_orgao — nenhum teste dbt cobria
-- isso até 26/07/2026. dbt falha se esta query retornar linhas.
select codigo_orgao, ano_previsto, trimestre_previsto, count(*) as linhas
from {{ source('ml_scores', 'previsao_pagamento_orgao') }}
group by codigo_orgao, ano_previsto, trimestre_previsto
having count(*) > 1
