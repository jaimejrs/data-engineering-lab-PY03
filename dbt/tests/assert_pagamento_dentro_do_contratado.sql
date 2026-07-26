-- Teste singular WARN: sinaliza contrato pago/empenhado a mais do que o
-- DOBRO do valor_contrato registrado, EXCLUINDO os casos já explicados por
-- aditivo/ajuste contratual (valor_contrato + valor_aditivo + valor_ajuste ≈
-- valor_pago, tolerância 1%) — essas duas colunas foram propagadas da
-- Silver em 26/07/2026 justamente pra afinar esse teste (achado: explicam
-- ~49% dos casos de valor_pago > valor_contrato). O que sobra depois de
-- excluir o explicado é o que realmente vale a pena olhar: pago/empenhado
-- muito acima do contratado SEM aditivo/ajuste que justifique — não temos
-- como confirmar contra a fonte original, então isso fica como
-- monitoramento, não correção automática.
{{ config(severity='warn') }}
select
    id_contrato_origem,
    ano,
    valor_contrato,
    valor_pago,
    valor_empenhado,
    valor_aditivo,
    valor_ajuste
from {{ ref('fato_contrato') }}
where valor_contrato > 0
    and (valor_pago > valor_contrato * 2 or valor_empenhado > valor_contrato * 2)
    and abs(
        (valor_contrato + coalesce(valor_aditivo, 0) + coalesce(valor_ajuste, 0)) - valor_pago
    ) > 0.01 * valor_pago
