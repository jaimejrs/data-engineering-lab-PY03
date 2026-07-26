-- Teste singular: chave de negócio única em score_anomalia_contrato — nenhum
-- teste dbt cobria isso até 26/07/2026, apesar de ser consumida por
-- fato_contrato como se fosse 1 linha por (id_contrato_origem, ano). dbt
-- falha se esta query retornar linhas. (previsao_pagamento_orgao tem chave de
-- 3 colunas — teste em arquivo separado, assert_previsao_chave_unica.sql,
-- pra não misturar shape de linha diferente no mesmo UNION.)
select id_contrato_origem, ano, count(*) as linhas
from {{ source('ml_scores', 'score_anomalia_contrato') }}
group by id_contrato_origem, ano
having count(*) > 1
