-- Teste singular WARN (não error, ao contrário de assert_valores_nao_negativos):
-- `valor_empenhado` negativo em fato_contrato pode ser um estorno/anulação de
-- empenho contábil legítimo (real na fonte), não necessariamente erro — por
-- isso fica monitorado aqui, não bloqueando o build. Achado da análise
-- crítica de 26/07/2026: 1 caso real (contrato 531938, -R$17.969,20).
{{ config(severity='warn') }}
select id_contrato_origem, ano, valor_contrato, valor_pago, valor_empenhado
from {{ ref('fato_contrato') }}
where valor_empenhado < 0
