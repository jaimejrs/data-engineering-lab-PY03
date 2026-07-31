# Mapa do código — o que cada arquivo faz

Referência arquivo a arquivo do código-fonte do projeto, organizada pela mesma
estrutura de diretórios do repositório. Não repete o que já está detalhado em
outros documentos — para o significado de cada **coluna** de cada tabela
Iceberg, ver `docs/12-dicionario-dados-colunas-iceberg.md` (interno); para o
diagrama de arquitetura, `diagrama-arquitetura.md`; para o racional de escolha
de cada ferramenta, `stacks/*.md` (interno).

---

## `dags/` — orquestração (Airflow, 4 DAGs encadeadas por Dataset)

### `dags/common.py`
Constantes e os 3 `Dataset` (`bronze://validated`, `silver://ready`,
`gold://ready`) compartilhados entre as DAGs — isolados aqui, sem nenhum
`@dag`, de propósito: importar um arquivo de DAG diretamente de outro fazia o
`DagBag` do Airflow registrar o mesmo `dag_id` duas vezes
(`AirflowDagDuplicatedIdException`, reproduzido em produção). Também guarda a
configuração do `spark-submit` client-mode (jars extras, host do driver) e o
nome da rede Docker onde o `DockerOperator` do dbt roda.

### `dags/dag_bronze_extract.py`
**DAG 1** (`bronze_extract`, `@daily`). Tasks: `extract_postgres` +
`extract_api` (em paralelo) → `validate` → `advance_watermark`. Cada fonte tem
sua própria *watermark de evento* (maior `dataemissao`/`data_assinatura` já
vista), com um lookback de 7 dias (`BRONZE_LOOKBACK_DAYS`) para capturar
lançamento retroativo na origem — o `MERGE INTO` da Silver torna reprocessar a
sobreposição seguro. `validate` chama `validate_bronze()` e, como efeito
colateral não-crítico, grava a contagem para a reconciliação Bronze→Silver
(`record_bronze_counts`) sem derrubar a DAG se isso falhar. Emite
`bronze://validated` ao final.

### `dags/dag_silver_transform.py`
**DAG 2** (`silver_transform`), disparada pelo Dataset da DAG 1. Uma única
task (`transform`) via `DockerOperator` rodando `silver_job.py` em
`spark-submit local[*]` dentro da imagem `datalab-spark:local` — escolhido em
vez de `SparkSubmitOperator` client-mode porque a imagem do Airflow não é um
bom runtime Spark e o client-mode entre containers dava problema de rede de
executor. Emite `silver://ready`.

### `dags/dag_gold_load.py`
**DAG 3** (`gold_load`), disparada pelo Dataset da DAG 2. Uma task
(`dbt_build`) via `DockerOperator` rodando `dbt build` na imagem
`datalab-dbt:local` — substituiu a carga imperativa antiga
(`gold_job.py`/`dw_loader.py`, hoje legado/removido). Emite `gold://ready`.

### `dags/dag_ml_inference.py`
**DAG 4** (`ml_inference`), disparada pelo Dataset da DAG 3. `score_anomalias`
e `prever_pagamentos` rodam em paralelo (via `PythonOperator`, import direto
de `models/`, sem container extra); `score_anomalias` depois dispara
`refresh_fato_contrato` (`DockerOperator`, `dbt build --select fato_contrato`)
para o score aparecer no fato ainda nesta mesma execução, já que
`fato_contrato` é recriada do zero a cada build (não incremental).
`gerar_relatorio_narrativo` roda por último, sem depender do refresh.

---

## `src/` — extração (Bronze), transformação (Silver) e utilitários

### `src/extractors/api_extractor.py`
Extração paginada da API REST do Ceará Transparente (contratos). Converte
data ISO para `DD/MM/YYYY` (formato real exigido pela API — ISO faz a API
responder texto de erro em vez de JSON), trata o typo real da API
(`"sumary"` em vez de `"summary"`, com fallback), e aborta em vez de iterar
infinitamente se a resposta não trouxer `total_pages`. Retorna só metadados
leves (contagens, maior `data_assinatura` vista) — nunca os registros —
seguro para XCom do Airflow.

### `src/extractors/postgres_extractor.py`
Extração bruta de `empenhos`, `ordem_bancaria_orcamentaria` e
`unidade_gestora` do Postgres de origem, em chunks (`POSTGRES_EXTRACT_CHUNK_SIZE`,
default 20.000 linhas) via cursor server-side (`stream_results=True` —
sem isso o Postgres tenta montar o resultado inteiro antes de mandar
qualquer linha, e estoura memória para as tabelas de ~1,3M linhas). Nenhuma
tabela do escopo tem PK real na origem.

### `src/extractors/storage.py`
Camada de escrita/leitura da Bronze, com backend trocável por variável de
ambiente (`local` para disco, `hdfs` via WebHDFS) sem acoplar o resto do
código a um dos dois. Também localiza recursivamente diretórios
`data_extracao=<data>` sob a raiz de uma fonte, cobrindo tanto o layout plano
(`contratos/`) quanto o particionado por `ano=/mes=` (`empenhos/`).

### `src/transformers/rules.py`
Regras de normalização/dedup compartilhadas entre a Silver pandas legada e o
job PySpark real — só stdlib (`re`/`datetime`), sem dependência pesada, para
poder ser importado dos dois lados. É a fonte única de verdade sobre quais
campos são data por fonte, qual coluna particiona cada tabela e qual é a
chave lógica de dedup (`DEDUP_KEYS`) na ausência de PK real.

### `src/spark_jobs/silver_job.py`
**O job real da Silver em produção** — lê a Bronze (JSON), normaliza data e
CNPJ/CPF, tipa colunas monetárias/de data, deduplica por `DEDUP_KEYS` e faz
`MERGE INTO` na tabela Iceberg correspondente (idempotente entre execuções).
Substituiu o caminho pandas (`silver_transformer.py`, removido — nenhuma DAG
o importa mais). Roda via `spark-submit --run-date <data>`.

### `src/spark_jobs/spark_session.py`
Fábrica única da `SparkSession` configurada para o catálogo Iceberg sobre
Hive Metastore/HDFS — todos os hostnames/URIs parametrizáveis por variável de
ambiente. Suporta também `ICEBERG_CATALOG_TYPE=hadoop` (sem metastore, cada
diretório é o catálogo), usado só pelos testes locais de idempotência do
`MERGE INTO` (`tests/test_silver_job_spark.py`), que rodam 100% sem
HMS/HDFS reais.

### `src/validators/bronze_validator.py`
Valida schema (colunas obrigatórias presentes e não-vazias) e completude
mínima da Bronze para uma `data_extracao`, por fonte. Usa amostragem
opcional (`BRONZE_VALIDATE_SAMPLE_SIZE`, início+fim+aleatório do meio) para
não custar O(todos os registros) na carga histórica completa, mas a
contagem total retornada é sempre exata. Levanta `BronzeValidationError` na
primeira fonte inválida — é o que a task `validate` da DAG 1 invoca.

### `src/reconciliation.py`
Grava, como efeito colateral da validação da Bronze (não uma releitura),
a contagem de registros por `(fonte, data_extracao)` em
`iceberg.audit.bronze_ingestao` — usada pelo modelo dbt
`bronze_silver_reconciliacao.sql` para comparar Bronze somada contra a
contagem ao vivo da Silver.

### `src/trino_io.py`
Conexão/consulta/escrita compartilhadas com o Trino, usadas pelos modelos de
ML e pela reconciliação. `bulk_insert` faz `INSERT ... VALUES` em lotes (sem
`executemany` no driver Trino). `replace_table` implementa troca atômica de
tabela via staging + `ALTER TABLE ... RENAME TO` — corrige um bug real onde
a tabela final ficava vazia/parcialmente escrita durante toda a gravação de
um re-score.

### `src/mlflow_utils.py`
Configura o tracking do MLflow (backend em arquivo local por padrão,
`models/artifacts/mlruns/`, sobrescrevível via `MLFLOW_TRACKING_URI` para
um servidor real). Uma função, chamada uma vez no início de cada modelo.

---

## `dbt/` — transformação declarativa da Gold (dbt-trino)

### `dbt/dbt_project.yml`
Config do projeto: `staging` é `ephemeral` (CTE inline, sem objeto no
catálogo), `marts` é `table` (Iceberg físico). O hook `on-run-start` cria,
se não existirem, as tabelas `ml.score_anomalia_contrato` e
`audit.bronze_ingestao` — ambas escritas por scripts Python, não pelo dbt —
para o projeto não quebrar com "table not found" num ambiente novo onde
esses scripts ainda não rodaram.

### `dbt/profiles.yml`
Perfil de conexão Trino (sem autenticação, HTTP) — host/porta via
`env_var()` com default para produção (`trino:8080`), sobrescrito só pelo
job de CI (`localhost`, já que o dbt roda no runner, não em container).

### `dbt/Dockerfile`
Imagem isolada só com `dbt-trino` (evita conflito de dependência com o
Airflow). Instala via `--only-binary` (o servidor não alcança `github.com`,
só PyPI). Projeto embutido via `COPY . /dbt`, sobreposto em runtime pelo
bind-mount do compose.

### `dbt/macros/surrogate_key.sql`
Gera as surrogate keys (`sk_*`) das dimensões: `md5` de todas as colunas da
chave concatenadas com separador, em hex minúsculo. Determinística — a
mesma chave de negócio sempre gera o mesmo hash, permitindo casar `sk_*`
entre dimensão e fato sem sequence (Iceberg não tem `BIGSERIAL`). Escolhida
para não depender de `dbt_utils` (evita `dbt deps`/egress).

### `dbt/macros/generate_schema_name.sql`
Override do macro padrão do dbt: usa o `custom_schema_name` exatamente como
declarado (`audit`, `ml`), em vez de concatenar com o schema do profile
(`gold_audit`) — é o que permite a separação física por propósito
(gold/ml/audit) no mesmo catálogo `iceberg`.

### `dbt/models/sources.yml`
Declaração das fontes (`silver.*`, `ml_scores.score_anomalia_contrato`,
`audit.*`) com testes `not_null`/`unique` nas chaves de negócio — a
primeira camada de teste automatizado que existe sobre a Silver/ml/audit.

### `dbt/models/staging/stg_*.sql` (4 arquivos)
Um por fonte (`stg_contratos`, `stg_empenhos`, `stg_ordem_bancaria`,
`stg_unidade_gestora`) — `SELECT` das colunas da Silver que a Gold
realmente consome, materializado como `ephemeral` (vira CTE inline, sem
objeto físico). Não fazem transformação além de seleção de coluna — a
Silver já normalizou.

### `dbt/models/marts/dim_*.sql` (4 arquivos: `dim_credor`, `dim_orgao`, `dim_modalidade`, `dim_tempo`)
Dimensões do modelo estrela. `dim_credor` é materializada a partir do
snapshot SCD2 (`scd_credor`); as outras três são `SELECT DISTINCT` +
surrogate key sobre a(s) staging correspondente(s). `dim_tempo` deriva
ano/trimestre/mês/dia-da-semana a partir da união de todas as datas de
evento (assinatura de contrato, emissão de empenho/OB).

### `dbt/models/marts/fato_*.sql` (3 arquivos: `fato_contrato`, `fato_empenho`, `fato_ordem_bancaria`)
Fatos do modelo estrela, um por estágio da despesa pública (contrato →
empenho → ordem bancária/pagamento efetivo). Cada um dedupa por chave de
negócio (`row_number()`), junta com as dimensões via `sk_*` e é particionado
por `ano`. `fato_contrato` inclui o `LEFT JOIN` com
`ml.score_anomalia_contrato` (`score_anomalia`, `NULL` até o modelo rodar) e
o join point-in-time com `dim_credor` (versão vigente na
`data_assinatura`, não a versão atual).

### `dbt/models/marts/bronze_silver_reconciliacao.sql` / `gold_reconciliacao.sql`
Telemetria de qualidade, materializadas fisicamente em `iceberg.audit`
(apesar de morarem na pasta `marts/`). A primeira compara o total validado
na Bronze (gravado por `src/reconciliation.py`) com a contagem ao vivo da
Silver; a segunda mede cobertura de join Silver→Gold por fato (% de linhas
sem `sk_orgao`/`sk_credor`/etc.).

### `dbt/models/marts/schema.yml`
Testes column-level da Gold: `not_null`/`unique` nas surrogate keys,
`accepted_values` no `tipo` de `dim_credor` (`PF`/`PJ`/`INVALIDO`), e
`relationships` de cada `sk_*` dos fatos contra a dimensão correspondente
(garante que nenhuma FK aponta para uma dimensão inexistente).

### `dbt/snapshots/scd_credor.sql`
Snapshot SCD2 (`strategy=check`) sobre `cnpj_cpf`, monitorando mudança em
`nome`/`tipo`/`historico_infringement`. `historico_infringement` é calculado
agregando `bool_or(infringement_status > 0)` por **todos** os contratos
daquele credor (corrigido em 25/07/2026 — antes considerava só um contrato
escolhido por dedup arbitrário).

### `dbt/tests/*.sql` (10 testes singulares)
- `assert_silver_chaves_unicas` / `assert_ml_chaves_unicas` /
  `assert_previsao_chave_unica` / `assert_bronze_ingestao_chave_unica` /
  `assert_fatos_chave_unica` — unicidade de chave composta (a fonte não tem
  PK declarada) nas 3 tabelas Silver incrementais, no output dos 2 modelos
  de ML e nos 3 fatos da Gold, respectivamente.
- `assert_valores_nao_negativos` — **ERROR**: nenhum valor monetário
  negativo em nenhum fato.
- `assert_empenho_negativo_monitorado` — **WARN**: `valor_empenhado`
  negativo pode ser estorno contábil legítimo (1 caso real conhecido),
  monitorado, não bloqueia o build.
- `assert_pagamento_dentro_do_contratado` — **WARN**: contrato pago/empenhado
  a mais do que o dobro do valor contratado, excluindo os casos já
  explicados por aditivo/ajuste contratual (~49% dos casos).
- `assert_reconciliacao_bronze_silver` — **WARN**: a Silver deduplicada
  nunca deveria superar a soma da Bronze validada.
- `assert_cobertura_gold_minima` — **WARN**: alerta se a reconciliação
  Silver→Gold ou a cobertura de join fugir de ~0,1% (limiar em 1%, 10x o
  baseline observado).

---

## `models/` — Machine Learning e IA generativa (Fase 3)

### `models/anomaly_detection.py`
**Modelo 1.** `IsolationForest` (`n_estimators=200`, `contamination="auto"`),
não supervisionado. Lê `fato_contrato` + dimensões + `silver.contratos`
(para `tipo_objeto`/vigência) via Trino, produz `score_anomalia` (`[0,1]`,
via `MinMaxScaler` sobre `-decision_function`) e `flag_anomalia`
(`predict() == -1`). Grava em `iceberg.ml.score_anomalia_contrato` via
`trino_io.replace_table` (substitui a tabela inteira, não incremental).
Loga hiperparâmetros/métricas/modelo no MLflow.

### `models/payment_forecast.py`
**Modelo 2.** Três `XGBRegressor` (um por quantil 0.1/0.5/0.9,
`objective="reg:quantileerror"`, `max_depth=4`, `learning_rate=0.05`). Lê
`fato_ordem_bancaria` (pagamento efetivo, não `fato_empenho`) agregado por
`codigo_orgao`/trimestre — agrupamento por `codigo_orgao`, não `sk_orgao`,
corrige um bug real onde a versão anual de `dim_orgao` quebrava os lags
entre anos. Grava em `iceberg.ml.previsao_pagamento_orgao`.

### `models/narrative_report.py`
**Componente de IA generativa.** Lê os outputs dos dois modelos acima via
Trino, monta um prompt só com os números já calculados (o LLM não recebe
dado bruto nem infere valor novo, para evitar alucinação) e usa a API
OpenAI (`gpt-4o-mini` por padrão) para escrever um relatório em Markdown em
linguagem sem jargão técnico. Grava em `iceberg.ml.relatorio_narrativo` e em
arquivo (`models/artifacts/relatorios/`).

---

## `streamlit/` — painel de negócio

Docker próprio (`streamlit/Dockerfile`, `COPY . .` só deste diretório — não
enxerga `src/`, por isso alguns módulos abaixo duplicam lógica que também
existe no pipeline principal em vez de importá-la). Consome
`iceberg.gold/ml` via Trino, com cache de 5min (`@st.cache_data`). Hospedado
no servidor via Docker Compose (`deploy/server-lakehouse/docker-compose.yml`,
serviço `streamlit`) e exposto publicamente por **Tailscale Funnel**
(`tailscale funnel --bg 8501`) — qualquer pessoa acessa pelo link, sem
precisar estar na tailnet do time (diferente de todo o resto do stack).

### `streamlit/app.py`
Entrada: carrega `.env` (raiz do projeto + `streamlit/.env` local, se
existir), monta a sidebar (filtros globais `Ano`/`Órgão`), calcula o
`score_threshold` do Modelo 1 (percentil dinâmico, não fixo — ver
`config.py`) e o indicador "dado disponível até" (`MAX` de
`fato_ordem_bancaria`, mostrado no topo de toda aba). Chama as 4
`.render()` das abas incondicionalmente — o modelo de execução do Streamlit
reroda o script inteiro a cada interação, então qualquer clique em qualquer
aba reexecuta as 4 (mitigado pelo cache de 5min do `db.run_query`).

### `streamlit/tabs/` — uma aba por módulo
- `visao_geral.py` — KPIs agregados, série mensal (com truncamento de meses
  com dado incompleto na ponta — ver `_ultimo_periodo_completo`), top 10
  órgãos por valor pago, drill-down por órgão.
- `previsao.py` — Modelo 2 (previsão trimestral), compara com
  `fato_ordem_bancaria` real (não `fato_contrato.valor_pago` — ver
  docstring do módulo para o porquê), inclui a previsão retroativa
  (`is_backtest`) pra comparar previsto vs. realizado em trimestres já
  fechados.
- `anomalias.py` — Modelo 1 (score de anomalia), distribuição, ranking de
  órgãos por contratos atípicos, tabela final com drill-down por clique no
  histograma.
- `resumo_ia.py` — relatório narrativo sob demanda (`ai_report.py`), prévia
  estática antes do clique, exportação em PDF (`pdf_export.py`).

### `streamlit/db.py`
Conexão com o Trino (reimplementa, não importa, o padrão de
`src/trino_io.py` — ver nota do Docker isolado acima). `run_query` cacheado
5min via `@st.cache_data`; converte colunas `DECIMAL` do driver Trino
(`decimal.Decimal`) para `float`.

### `streamlit/config.py` / `formatting.py` / `sql_filters.py` / `style.py`
Constantes de conexão/paleta institucional (verde/amarelo do Governo do
Ceará); formatação de valores (R$ bi/mi/mil) e classificação de risco,
compartilhadas entre as abas; fragmentos SQL dos filtros globais; CSS
custom — ver comentários no próprio `style.py` sobre por que os seletores
precisam ser `div[data-testid="stSelectbox"] div[role="group"]` (React
Aria, não BaseWeb) e por que o Streamlit está **pinado** (`==1.60.0`, não
`>=`) em `requirements.txt`: um rebuild anterior puxou uma versão nova sem
aviso e quebrou o CSS de contraste dos filtros silenciosamente.

### `streamlit/ai_report.py`
Mesmo padrão anti-alucinação de `models/narrative_report.py` (LLM só
reescreve números já calculados, nunca infere) — duplicado aqui, não
importado, pelo mesmo motivo do Docker isolado. `_valores_nao_verificados`
confere todo valor em R$ do texto gerado contra o conjunto de valores
realmente injetados no prompt.

### `streamlit/pdf_export.py`
Markdown → HTML → PDF via `markdown` + `xhtml2pdf`, 100% Python — escolhido
especificamente para não precisar de `wkhtmltopdf`/Chromium (`apt-get`) no
Dockerfile do painel.

### `streamlit/tests/test_app_smoke.py`
Teste de fumaça (`AppTest`) que carrega as 4 abas com `db.get_connection`
mockado (não `db.run_query` — evita a armadilha de `from db import
run_query` copiar uma referência antiga) e garante que nenhuma exceção
Python é levantada. Não simula o clique em "Gerar relatório com IA" de
propósito (chamada real à OpenAI). Roda no job `streamlit-smoke` do CI —
motivado por uma regressão real (upgrade silencioso do Streamlit quebrando
o CSS sem nenhum teste pegar).

---

## `deploy/server-lakehouse/` — deploy e operação em produção

### `auto-sync.py`
CD pull-based: roda em cron a cada 15min, pergunta à Checks API do GitHub
se o commit mais novo de `main` já tem CI verde e só então aplica
`sync-from-git.sh --apply`. Desenho deliberado para nunca guardar chave SSH
nos segredos do GitHub Actions — o servidor pergunta ao GitHub, o GitHub
nunca entra no servidor. Nunca reaplica o mesmo commit duas vezes (arquivo
de estado) e confere erro de import de DAG depois de aplicar.

### `sync-from-git.sh`
`git pull` num clone read-only (`~/repo`) + `rsync --delete` dos
subdiretórios relevantes (`dbt/`, `src/`, `dags/`, `models/`) para os
diretórios "vivos" (`~/lakehouse/`, `~/airflow/`), preservando artefatos
gerados (logs, `mlruns`, `.joblib`). Modo dry-run por padrão; `--apply`
aplica de verdade.

### `maintenance.sh`
Rotina de manutenção do Iceberg, agendada via cron diário: `optimize`
(compaction) — por partição de ano nas tabelas grandes, para não repetir o
OOM que derrubava o Trino ao compactar a tabela inteira —, `expire_snapshots`
e `remove_orphan_files` em todas as tabelas Silver/Gold/ml/audit, mais
retenção por linha (`DELETE`) nas tabelas de auditoria sensíveis
(`AUDIT_RETENTION_DAYS`, padrão 90 dias) e de métricas de infra
(`METRICS_RETENTION_DAYS`, padrão 30 dias).

### `collect_infra_metrics.py`
Roda via cron a cada 5min: `docker stats` (CPU/memória por container) +
`df` (disco por ponto de montagem), grava em
`iceberg.audit.infra_metricas_containers`/`infra_metricas_disco`. Consumido
pelo painel "Métricas de Infraestrutura" do Superset.

### `collect_access_audit.py`
Roda via cron a cada 5min: `journalctl` (login/logout SSH) + `auditd`/`ausearch`
(comando executado, mesmo via `sudo`, filtrado para excluir processo
interno de container/cron) — grava em `iceberg.audit.sessoes_ssh`/
`comandos_executados`. Existe porque várias pessoas compartilham acesso ao
mesmo servidor com o mesmo login.

### `docker-compose.yml` (overlay)
Overlay aditivo só com os serviços do lakehouse (Hive Metastore, Spark,
Trino, Superset, dbt) — soma à stack pré-existente do time no servidor
(Postgres, Postgres DW, Airflow, Hadoop), na mesma rede Docker.

---

## `docker/` — imagens do stack

### `docker/airflow/Dockerfile`
`apache/airflow:2.9.1` + Java 17 (JRE, para o `spark-submit` client-mode) +
jars do Iceberg/driver JDBC Postgres embutidos (via `ADD`, não baixados em
runtime — egress restrito) + symlink estável para o `spark-submit` que vem
dentro do wheel do `pyspark`.

### `docker/hive/Dockerfile` + `entrypoint.sh`
Hive Metastore standalone (catálogo Iceberg), backing store no Postgres. O
entrypoint roda `schematool -initSchema` de forma idempotente (só se o
schema ainda não existir) antes de subir o serviço em foreground.

### `docker/spark/Dockerfile`
`apache/spark:3.5.3` — mesma versão usada no `pyspark` do `requirements.txt`
e no Airflow, para não haver mismatch de versão entre driver e executor.

### `docker/trino/Dockerfile`
`trinodb/trino:455` + catálogo Iceberg + config de HDFS parametrizada por
build-arg (`HDFS_HOST`).

### `docker/superset/Dockerfile` + `superset_config.py`
Instala `trino`/`sqlalchemy-trino` no venv correto da imagem base (que usa
`uv`, não `pip` — um `pip install` comum iria para fora do venv real). A
config aponta o backing store do Superset pro Postgres compartilhado, sem
Redis/Celery (painel interno pequeno não precisa de cache assíncrono).

### `docker/postgres-init/01-metastore.sh` / `02-superset.sh`
Scripts de inicialização do Postgres (`docker-entrypoint-initdb.d`), criam
o usuário/DB dedicado do Hive Metastore e do Superset respectivamente — só
rodam quando o volume do Postgres é criado do zero (documentado o runbook
manual para quando o volume já existe).

---

## `.github/` — CI/CD

`workflows/ci.yml` (6 jobs de CI + 1 de deploy — `lint`, `tests`,
`streamlit-smoke`, `spark-tests`, `dbt`, `dbt-integration`) e `.github/ci/*`
(imagens e seed sintético usados só pelo job `dbt-integration`) — detalhado
a fundo em `stacks/github-actions-cicd.md` (interno), não repetido aqui.

---

## `tests/` — cobertura automatizada

Um arquivo por módulo de `src/`/`models/`/`deploy/server-lakehouse/`
correspondente (`test_extractors.py`, `test_storage.py`,
`test_bronze_validator.py`, `test_reconciliation.py`, `test_trino_io.py`,
`test_silver_job_spark.py`, `test_anomaly_detection.py`,
`test_payment_forecast.py`, `test_narrative_report.py`, `test_auto_sync.py`,
`test_collect_infra_metrics.py`, `test_collect_access_audit.py`,
`test_compose_sync.py` — este último compara os dois `docker-compose.yml`
do projeto para pegar drift entre o stack autônomo e o overlay do
servidor). `test_silver_job_spark.py` é o único que precisa de Spark local
de verdade (roda via job dedicado no CI, `spark-tests`); os demais são
Python puro/mocks.

---

## Raiz do repositório

### `docker-compose.yml`
Stack completo autônomo (Postgres, Hadoop, Airflow, Jupyter, Spark, Hive
Metastore, Trino, Superset) — usado para rodar o projeto do zero fora do
servidor real do time.

### `documentacao/indices-banco-origem.sql`
Script de referência com os índices recomendados para as colunas de data
usadas como filtro incremental no Postgres de origem (`dataemissao`) —
documentação de uma sugestão de otimização, não aplicada automaticamente
por nenhum pipeline.
