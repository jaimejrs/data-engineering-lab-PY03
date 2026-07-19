-- Índices aplicados no PostgreSQL de origem (externo, gerenciado pelo curso)
-- em 19/07/2026, para resolver o item do checklist "extract_postgres performático
-- em backfills grandes (lento por falta de índice em dataemissao)".
--
-- Contexto: nenhuma tabela do banco de origem tem PK/índice declarado. Mesmo
-- uma extração incremental de 1 mês (5-7 mil linhas de ~1,4 milhão) fazia
-- Seq Scan completo — ~2,4s e ~325 mil buffers lidos por tabela, a cada
-- execução da DAG. Ver EXPLAIN ANALYZE antes/depois em
-- `relatorio-progresso-fase1-2.md`.
--
-- CONCURRENTLY evita lock na tabela (banco compartilhado com outras equipes
-- do curso). Reaplicar este script se o banco de origem for reprovisionado.
--
-- Resultado: consultas incrementais (range pequeno) passaram a usar Index
-- Scan (~400ms, ~1300 buffers) em vez de Seq Scan. Backfills que cobrem quase
-- toda a tabela continuam corretamente em Seq Scan — decisão do planner,
-- esperada e correta para esse caso.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empenhos_dataemissao
    ON empenhos (dataemissao);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ordem_bancaria_orcamentaria_dataemissao
    ON ordem_bancaria_orcamentaria (dataemissao);

-- Verificação:
-- SELECT indexrelid::regclass, indisvalid FROM pg_index
--   WHERE indexrelid::regclass::text LIKE 'idx_%_dataemissao';
