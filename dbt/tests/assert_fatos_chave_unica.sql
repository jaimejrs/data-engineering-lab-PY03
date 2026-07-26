-- Teste singular: chave de negócio única nos 3 fatos — a dedup
-- (row_number() over (partition by <chave> order by ... desc) where rn = 1)
-- garante isso hoje, mas nenhum teste confirmava; uma regressão futura na
-- lógica não seria pega. dbt falha se esta query retornar linhas.
select 'fato_contrato' as tabela, id_contrato_origem as chave, ano, count(*) as linhas
from {{ ref('fato_contrato') }}
group by id_contrato_origem, ano
having count(*) > 1

union all
select 'fato_empenho', cast(id_empenho_origem as varchar), ano, count(*)
from {{ ref('fato_empenho') }}
group by id_empenho_origem, ano
having count(*) > 1

union all
select 'fato_ordem_bancaria', cast(id_ob_origem as varchar), ano, count(*)
from {{ ref('fato_ordem_bancaria') }}
group by id_ob_origem, ano
having count(*) > 1
