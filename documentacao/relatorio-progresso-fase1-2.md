# Relatório de Progresso — Fases 1 e 2

**Este NÃO é o relatório final exigido pelo §6.2 do `Trabalho Final.pdf`** (que pede análise
dos resultados dos modelos de ML — Fase 3, ainda não iniciada). É um relatório de status,
para acompanhamento do andamento real do projeto frente ao cronograma.

Data de referência: 19/07/2026 · Autor: Jaime (orquestração) · Fonte: inspeção direta do
Airflow/HDFS/PostgreSQL no servidor do Datalab + `docs/checklist.md` interno.

## Resumo executivo

O projeto está na Fase 1 (Bronze), que agora roda 100% dentro do Airflow ponta a ponta pela
primeira vez (DAG `bronze_extract`, 4/4 tasks com sucesso). A Fase 2 (Silver/Gold) ainda não
foi iniciada. O cronograma original previa a Fase 2 concluída em 19/07/2026 — as tarefas que a
bloqueiam (10, 13, 15) ainda não começaram, então esse prazo já não será cumprido.

## O que está funcionando

- **Extração da API de contratos** — validada com 30.036 contratos reais gravados no HDFS
  (`/bronze/contratos/data_extracao=2026-07-17`) e, desde 19/07, rodando dentro da própria DAG
  (ver correção 3 abaixo e o aviso de dependência em "Bloqueios conhecidos").
- **Extração das tabelas do PostgreSQL de origem** (`empenhos`, `ordem_bancaria_orcamentaria`,
  `unidade_gestora`) — validada ponta a ponta no HDFS real, com watermark incremental via
  Airflow Variable (`bronze_last_data_extracao`).
- **DAG `bronze_extract`** — criada e implantada no Airflow real, com tasks separadas
  (`extract_postgres`, `extract_api`, `validate`, `advance_watermark`) e retries configurados.
- **Infraestrutura base** — Postgres, Hadoop (NameNode/DataNode) e Airflow rodando no
  servidor do Datalab (`datalab-server`).

## Correções aplicadas em 18–19/07/2026

1. **Regressão na task `extract_postgres`** (18/07): as 4 últimas execuções da DAG (17–18/07)
   falhavam com `ModuleNotFoundError: No module named 'hdfs'`. Causa raiz: as dependências
   Python da imagem do Airflow (`hdfs`, `psycopg2-binary`, `pandas` etc.) tinham sido
   instaladas manualmente dentro do container em execução, não na imagem — não sobrevivem a
   uma recriação do container. Corrigido com uma imagem customizada (`docker/airflow/Dockerfile`)
   que instala essas dependências de forma reproduzível.
2. **`docker-compose.yml` do repositório** (18/07) — não existia (entrega explícita do §6.1 do
   PDF). Adicionado, cobrindo Postgres, Hadoop (NameNode + DataNode), Airflow (imagem custom) e
   Jupyter.
3. **Bloqueio de saída IPv4 para `extract_api`** (19/07) — resolvido dentro do Airflow. Causa
   raiz confirmada: o `datalab-server` não tem nenhuma rota IPv4 padrão (só IPv6) — não era um
   problema de configuração do Docker, e sim do host. Contornado com um relay TCP rodando no
   notebook `jotav15-1` (que tem IPv4 real), alcançado via Tailscale, sem tocar no roteamento
   do host — abordagem diferente da tentativa anterior com exit node (que tinha quebrado o
   roteamento local e foi revertida). Detalhes completos em
   [`workaround-egress-ipv4-api.md`](workaround-egress-ipv4-api.md). **Resultado:** DAG
   `bronze_extract` completou com sucesso 100% dentro do Airflow pela primeira vez
   (`manual__2026-07-19T10:52:47`, 526 contratos extraídos, 4/4 tasks `success`).
4. **Validador da Bronze quebrado pelo particionamento `ano=/mes=`** (19/07) — a Nara implementou
   e mergeou em `main` o particionamento por `ano=/mes=` em `empenhos`/`ordem_bancaria_orcamentaria`
   (commit `21a6f40`), o que fazia `validate_source` (que só olhava
   `{fonte}/data_extracao={data}/`) nunca encontrar arquivo nenhum. Corrigido com
   `find_data_extracao_dirs` em `src/extractors/storage.py` — busca recursiva que cobre tanto o
   layout plano (`contratos/`, `unidade_gestora/`) quanto o aninhado, sem hardcodar qual fonte usa
   qual. Validado contra os dados reais do HDFS (960 arquivos/1,38M registros em `empenhos` para
   uma data) e com a DAG rodando de novo ponta a ponta.
5. **`extract_postgres` lento em qualquer filtro de data** (19/07) — descoberto que
   `SOURCE_POSTGRES_URL` usa o usuário `postgres` (superuser real, não restrito como o relatório
   original supunha). Mesmo uma extração incremental de 1 mês fazia Seq Scan completo (~2,4s,
   ~325 mil buffers) por falta de índice em `dataemissao`. Criados
   `idx_empenhos_dataemissao`/`idx_ordem_bancaria_orcamentaria_dataemissao` via
   `CREATE INDEX CONCURRENTLY` (sem lock — banco compartilhado com outras equipes do curso) — ver
   [`indices-banco-origem.sql`](indices-banco-origem.sql). Confirmado com `EXPLAIN ANALYZE`
   antes/depois: consulta incremental típica caiu para Index Scan (~400ms, ~1.300 buffers).

## Bloqueios conhecidos (em aberto)

- **Dependência do relay em `jotav15-1`**: a correção do item 3 acima depende de uma máquina
  pessoal do time estar ligada e conectada ao Tailscale. Não é solução definitiva — ver as
  opções de resolução permanente em `workaround-egress-ipv4-api.md` (IPv4 real no provedor,
  ou mover o relay para uma máquina sempre ligada).
- **Tarefa 10** (mapeamento de dados/regras, Carlos e Fernanda) — ainda não iniciada,
  bloqueia as tarefas 13 e 15, que por sua vez bloqueiam toda a Fase 2.

## Riscos para o cronograma

- Fase 2 (Silver/Gold) tinha data-alvo de conclusão em 19/07/2026 — não iniciada; esse prazo
  já não será cumprido.
- Fase 3 (ML/IA) depende do DW (tarefa 16), que depende da tarefa 10 — efeito cascata.
- Code freeze previsto para 26/07/2026 é o ponto de risco mais visível caso a tarefa 10 não
  destrave a Fase 2 nos próximos dias.
- Risco residual (baixo, mas presente): se `jotav15-1` ficar indisponível perto da Fase 4
  (testes de ponta a ponta, 24–25/07), `extract_api` volta a falhar dentro do Airflow — sem
  perda de dados, só atraso.

## Próximos passos recomendados

1. Decidir o destino definitivo do relay de `extract_api` antes da Fase 4 (tarefa 26/27) —
   manter em `jotav15-1`, migrar para uma máquina sempre ligada, ou buscar IPv4 real com o
   provedor do `datalab-server`.
2. Priorizar a tarefa 10 (mapeamento), já que é o gargalo que atrasa toda a Fase 2.
3. Retomar este relatório como o **relatório final** do §6.2 assim que a Fase 3 (modelos de
   ML) estiver concluída — hoje ele documenta apenas status, não resultados analíticos.
