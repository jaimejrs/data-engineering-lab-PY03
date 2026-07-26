-- Teste singular: chave de negócio única (fonte, data_extracao) em
-- bronze_ingestao — src/reconciliation.py já faz DELETE+INSERT por essa
-- chave (upsert manual, ver record_bronze_counts), mas nenhum teste dbt
-- confirmava isso até 26/07/2026. dbt falha se esta query retornar linhas.
select fonte, data_extracao, count(*) as linhas
from {{ source('audit', 'bronze_ingestao') }}
group by fonte, data_extracao
having count(*) > 1
