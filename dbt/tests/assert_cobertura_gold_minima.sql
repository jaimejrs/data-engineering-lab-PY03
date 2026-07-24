-- Teste singular WARN (não ERROR — um drop pode ser legítimo, ex: órgão sem
-- match real na origem): alerta quando a reconciliação Silver -> Gold ou a
-- cobertura de join de um fato foge do esperado (~0,1% historicamente).
-- Ver dbt/models/marts/gold_reconciliacao.sql e docs/06-analise-critica.md
-- (itens 4 e 6). Limiar em 1% — 10x acima do baseline observado.
{{ config(severity='warn') }}
select *
from {{ ref('gold_reconciliacao') }}
where pct_descartadas > 1.0
   or pct_sem_orgao > 1.0
   or pct_sem_tempo > 1.0
