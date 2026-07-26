-- Teste singular WARN: sinaliza contrato pago/empenhado a mais do que o
-- DOBRO do valor_contrato registrado. Achado da análise crítica de
-- 26/07/2026: ~2,3% dos contratos (4.955 de 216.358) têm valor_pago >
-- valor_contrato — mas a grande maioria é overage modesto (até 100% a mais),
-- plausivelmente aditivo contratual não refletido de volta no
-- valor_contrato original (calculated_valor_pago/calculated_valor_empenhado
-- já vêm calculados assim pela API de origem, ver
-- documentacao/dicionario-dados.md — não é bug do nosso pipeline). O limiar
-- de 2x aqui é para focar a atenção nos casos mais extremos (961 contratos),
-- não em todo overage — não temos como confirmar aditivo contra a fonte
-- original, então isso fica como monitoramento, não correção automática.
{{ config(severity='warn') }}
select id_contrato_origem, ano, valor_contrato, valor_pago, valor_empenhado
from {{ ref('fato_contrato') }}
where valor_contrato > 0
  and (valor_pago > valor_contrato * 2 or valor_empenhado > valor_contrato * 2)
