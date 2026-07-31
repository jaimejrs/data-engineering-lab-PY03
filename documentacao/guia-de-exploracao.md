# Guia de exploração — como acessar cada etapa do projeto

Última atualização: 31/07/2026. Objetivo: dar a qualquer pessoa do time (ou
avaliador) o caminho mais curto pra **ver e mexer** em cada peça do pipeline —
não só ler o código, mas rodar uma query, abrir uma UI, ler um log real.

> Pré-requisito de rede para acessar o **servidor real** (`datalab-server`,
> `100.69.31.14`): estar na mesma tailnet **Tailscale** do time. Sem isso,
> nenhum endpoint abaixo (exceto os que rodam no seu próprio
> `docker compose up` local) responde. Ambiente de laboratório — sem senha no
> Trino/Hive Metastore, HTTP (não HTTPS); ver ressalvas de segurança em
> `docs/03-pendencias-e-melhorias.md` (interno).
>
> Tudo abaixo tem dois endereços possíveis: **local** (seu próprio
> `docker compose up -d --build`, `localhost`) e **servidor real do time**
> (`100.69.31.14`, containers já no ar). Use o que fizer sentido pro seu caso.

## Mapa rápido

| Etapa | Aplicação | Local | Servidor real |
|---|---|---|---|
| Orquestração | Airflow | `localhost:8080` | `100.69.31.14:8080` |
| Bronze (JSON bruto) | HDFS / WebHDFS | `localhost:9870` | `100.69.31.14:9870` |
| Silver / Gold (Iceberg) | Trino | `localhost:8085` | `100.69.31.14:8085` |
| ML — experimentos | MLflow (arquivo local) | `mlflow ui` apontando pra `models/artifacts/mlruns/` | idem, dentro do container `datalab_airflow_scheduler` |
| IA Generativa | Relatório narrativo | `models/artifacts/relatorios/*.md` + tabela Trino | idem |
| Transformação Gold | dbt docs | `cd dbt && dbt docs generate && dbt docs serve` | — (rodar localmente contra os artefatos) |
| Notebooks / EDA | Jupyter | `localhost:8888` (token `datalab`, ver `.env`) | — (rode local, aponte pro Trino do servidor) |
| Spark (backfills manuais) | Spark master UI | `localhost:8081` | interno à rede Docker do servidor |
| Painel de negócio | Streamlit | — (requer o overlay do servidor, não sobe no `docker-compose.yml` raiz) | `100.69.31.14:8501` **ou** link público (Tailscale Funnel, sem VPN — ver seção 8) |
| Painéis operacionais (infra/acesso) | Superset | — | interno à rede Docker do servidor |
| Qualidade de código | CI (GitHub Actions) | — | `gh run list` / aba **Actions** no GitHub |

---

## 1. Airflow — orquestração das 4 DAGs

**UI:** login `admin`/`admin` (local) ou o usuário configurado no servidor.

O que olhar:
- **Grid/Graph view** de `bronze_extract` → `silver_transform` → `gold_load` →
  `ml_inference` — cada DAG é disparada pela anterior via **Dataset** (ver a
  aba "Datasets" no menu do Airflow para visualizar quem produz/consome o
  quê).
- **Logs de cada task** — clique numa task no Grid → "Logs". É onde aparece,
  por exemplo, quantos registros a `validate` da Bronze contou, ou quantos
  contratos o `score_anomalias` escorou.
- **Variables** (menu Admin → Variables) — `bronze_last_data_extracao` é o
  watermark incremental; avança a cada execução bem-sucedida da DAG 1.

**CLI (dentro do container, útil pra debug sem esperar o scheduler):**
```bash
# local
docker exec datalab_airflow_scheduler airflow dags list
docker exec datalab_airflow_scheduler airflow dags trigger bronze_extract
docker exec datalab_airflow_scheduler airflow variables get bronze_last_data_extracao
docker exec datalab_airflow_scheduler airflow tasks states-for-dag-run <dag_id> <run_id>

# servidor (via SSH primeiro: ssh dataadm@100.69.31.14)
docker exec datalab_airflow_scheduler airflow dags list-runs -d ml_inference -o plain
```

---

## 2. Bronze — HDFS (JSON bruto)

**UI:** `http://<host>:9870` → *Utilities → Browse the file system* → `/bronze/<fonte>/...`.

**CLI (no servidor):**
```bash
ssh dataadm@100.69.31.14
docker exec aula_hadoop hdfs dfs -ls -R /bronze/contratos | head
docker exec aula_hadoop hdfs dfs -cat /bronze/contratos/data_extracao=2026-07-25/page_0001.json | head
```

**Python, de qualquer máquina (via WebHDFS):**
```python
import pandas as pd
from hdfs import InsecureClient                     # pip install hdfs
cli = InsecureClient("http://100.69.31.14:9870", user="root")
with cli.read("/bronze/contratos/data_extracao=2026-07-25/page_0001.json", encoding="utf-8") as f:
    df = pd.read_json(f)                             # arquivo Bronze = array JSON por arquivo
print(df.shape)
```

---

## 3. Silver e Gold — Iceberg via Trino

Catálogo `iceberg`; schemas `silver` (dado normalizado, deduplicado), `gold`
(modelo estrela), `ml` (saída dos modelos de IA) e `audit` (telemetria do
pipeline) — schemas separados desde 26/07/2026, antes tudo vivia junto em
`gold`. Principais tabelas: `gold.dim_credor` (SCD2), `gold.dim_orgao`,
`gold.dim_modalidade`, `gold.dim_tempo`, `gold.fato_contrato`,
`gold.fato_empenho`, `gold.fato_ordem_bancaria`, `ml.score_anomalia_contrato`,
`ml.previsao_pagamento_orgao`, `ml.relatorio_narrativo`,
`audit.bronze_ingestao`, `audit.gold_reconciliacao`.

**CLI Trino (no servidor):**
```bash
docker exec -it lakehouse_trino trino --catalog iceberg --schema gold
trino> SHOW TABLES;
trino> SELECT count(*) FROM fato_contrato;
trino> SELECT * FROM iceberg.silver.contratos LIMIT 10;

-- time travel (histórico de snapshots do Iceberg):
trino> SELECT * FROM iceberg.gold."fato_contrato$snapshots";

-- os 10 contratos mais atípicos segundo o Modelo 1:
trino> SELECT s.id_contrato_origem, s.score_anomalia, f.valor_contrato, o.nome
       FROM iceberg.ml.score_anomalia_contrato s
       JOIN gold.fato_contrato f ON f.id_contrato_origem = s.id_contrato_origem AND f.ano = s.ano
       LEFT JOIN gold.dim_orgao o ON f.sk_orgao = o.sk_orgao
       ORDER BY s.score_anomalia DESC LIMIT 10;

-- o relatório narrativo mais recente, por completo:
trino> SELECT gerado_em, llm_model, conteudo_markdown FROM iceberg.ml.relatorio_narrativo;
```

**Python (cliente `trino`, mesmo padrão usado por `src/trino_io.py`):**
```python
import pandas as pd
from trino.dbapi import connect
conn = connect(host="100.69.31.14", port=8085, user="analyst",
               catalog="iceberg", schema="gold", http_scheme="http")
df = pd.read_sql("SELECT * FROM fato_contrato LIMIT 1000", conn)
```

**Cliente gráfico (DBeaver/DataGrip) via JDBC:** driver `io.trino:trino-jdbc`,
URL `jdbc:trino://100.69.31.14:8085`, usuário qualquer, sem senha, SSL off.

---

## 4. MLflow — experimentos dos Modelos 1 e 2

Sem servidor MLflow dedicado no stack — o tracking usa **backend de arquivo
local**, gravado em `models/artifacts/mlruns/` (dentro do container, montado
em `/opt/airflow/models/artifacts/mlruns` no servidor).

**Abrir a UI do MLflow** (local, apontando pro diretório de tracking):
```bash
# de dentro do repo, com o venv ativado
mlflow ui --backend-store-uri file://$(pwd)/models/artifacts/mlruns
# abre em http://localhost:5000
```

Para ver os experimentos **do servidor**, copie a pasta antes (ou rode o
`mlflow ui` dentro do próprio container, publicando a porta):
```bash
# no servidor
docker exec -it datalab_airflow_scheduler bash
mlflow ui --backend-store-uri file:///opt/airflow/models/artifacts/mlruns --host 0.0.0.0 --port 5000
# depois, do seu lado: ssh -L 5000:localhost:5000 dataadm@100.69.31.14 (encaminha a porta via SSH)
```

O que olhar na UI: dois experimentos (`anomaly_detection`,
`payment_forecast`), cada run com os `params` do treino, as `metrics`
(score médio/máximo, percentis, contagem por threshold para o Modelo 1; MAE
e cobertura do intervalo para o Modelo 2) e o `.joblib` do modelo como
artefato baixável.

**Sem abrir a UI**, direto pelos arquivos:
```bash
find models/artifacts/mlruns -name "metrics" -type d
cat models/artifacts/mlruns/<experiment_id>/<run_id>/metrics/score_medio
```

---

## 5. IA Generativa — relatório narrativo

Cada execução da task `gerar_relatorio_narrativo` (DAG 4) grava o relatório
em dois lugares:

1. **Arquivo Markdown**, pronto pra ler: `models/artifacts/relatorios/relatorio_<timestamp>.md`
2. **Tabela Trino** (histórico + metadados): `iceberg.ml.relatorio_narrativo`
   — colunas `gerado_em`, `llm_model`, `num_contratos_anomalos`,
   `num_orgaos_previstos`, `conteudo_markdown`.

```bash
# no servidor
docker exec datalab_airflow_scheduler cat /opt/airflow/models/artifacts/relatorios/relatorio_<timestamp>.md
```

Gerar um novo relatório manualmente (requer `OPENAI_API_KEY` no `.env`):
```bash
python -m models.narrative_report --top-anomalias 10 --top-previsoes 10
```

---

## 6. dbt — modelos e documentação da Gold

```bash
cd dbt
dbt build              # roda modelos + testes (requer Trino no ar)
dbt docs generate       # gera o site de documentação (lineage, colunas, testes)
dbt docs serve           # abre em http://localhost:8080 (não conflita com Airflow
                          # se você rodar em outra porta: dbt docs serve --port 8081)
```

O site do `dbt docs` mostra o **lineage** completo (de qual `source`/`ref`
cada modelo vem), a descrição de cada coluna (`dbt/models/marts/schema.yml`)
e o resultado dos testes da última execução.

---

## 7. Notebooks — EDA e treino dos modelos

`notebooks/` (requer o stack no ar — Trino alcançável):

| Notebook | Conecta em | Para quê |
|---|---|---|
| `exploracao_ingestao.ipynb` | API + Postgres (fontes) | Inspecionar o dado antes da Bronze |
| `eda_bronze.ipynb` | Bronze (JSON) | Schema, nulos, formatos de data do dado bruto |
| `eda_silver.ipynb` | Silver via Trino | Volume, dedup, snapshots (time travel) |
| `eda_gold.ipynb` | Gold via Trino | Modelo estrela, cobertura de join, SCD2 |
| `eda_e_treinamento_ml.ipynb` | Gold+Silver via Trino | Treino/avaliação dos Modelos 1/2 + demonstração da IA generativa |

```bash
# local, com o stack no ar
docker compose up -d jupyter
# abra http://localhost:8888 (token: valor de JUPYTER_TOKEN no .env, default "datalab")
```

Pra apontar o Jupyter local pro **Trino do servidor**, defina `TRINO_HOST=100.69.31.14`
na primeira célula ou no `.env` montado no container.

---

## 8. Painel de negócio — Streamlit

4 abas (Visão Geral, Previsão de Pagamentos, Anomalias em Contratos, Resumo
IA), consumindo `iceberg.gold`/`iceberg.ml` via Trino. Só existe no overlay
do servidor (`deploy/server-lakehouse/docker-compose.yml`, serviço
`streamlit`) — não sobe com o `docker-compose.yml` da raiz.

**Acesso:**
- **Link público** (Tailscale Funnel — funciona de qualquer rede, sem VPN):
  `https://datalab-server.taila180c3.ts.net/`
- **Na tailnet do time:** `http://100.69.31.14:8501`

**CLI útil (no servidor, requer SSH):**
```bash
docker logs lakehouse_streamlit --tail 30
docker compose -f ~/lakehouse/docker-compose.yml restart streamlit  # após trocar .py (sem mudar requirements.txt)
docker compose -f ~/lakehouse/docker-compose.yml build streamlit    # após mudar requirements.txt
tailscale funnel status                                              # confirma a exposição pública ativa
```

Cache de 5min por query (`@st.cache_data` em `streamlit/db.py`) — mudanças no
dado (novo `dbt build`/re-treino de modelo) podem levar até 5min para
aparecer no painel sem precisar reiniciar o container.

---

## 9. Qualidade de código — lint, testes, CI

```bash
ruff check .              # lint (E/F/I/UP/B — ver pyproject.toml)
ruff format --check .      # formatação
python -m pytest tests/ -v # suíte completa (mocka Trino/OpenAI — não precisa do stack no ar)
```

CI no GitHub Actions (6 jobs a cada push): `lint` (ruff), `tests` (pytest
suíte leve), `streamlit-smoke` (`AppTest` do painel, Trino mockado),
`spark-tests` (idempotência do `MERGE INTO`, Spark+Iceberg local de
verdade), `dbt` (`dbt parse`, sem conectar no Trino), `dbt-integration`
(`dbt build` real contra Trino+Iceberg+HMS efêmero). Só em push na `main` e
só se os 6 passarem, roda o job de `deploy` (CD).

```bash
gh run list --limit 5      # últimas execuções
gh run view <run_id>        # detalhe de uma execução
```

---

## Resumo de conexões (servidor real)

| Ferramenta | Endpoint | Detalhe |
|---|---|---|
| Airflow | `100.69.31.14:8080` | `admin`/senha configurada |
| Trino (CLI/JDBC/Python) | `100.69.31.14:8085` | catálogo `iceberg`, schemas `silver`/`gold`, sem senha, HTTP |
| HDFS / WebHDFS (Bronze) | `100.69.31.14:9870` | UI ou `hdfs`/`InsecureClient(user="root")` |
| Postgres (metadados Airflow) | `100.69.31.14:5432` | uso interno — não é onde os dados de negócio ficam |
| Postgres DW (descontinuado) | `100.69.31.14:5434` | legado — Gold real hoje é Iceberg, não este banco |
| MLflow | sem porta fixa — `mlflow ui` sob demanda | backend de arquivo, `models/artifacts/mlruns/` |
| Painel de negócio (Streamlit) | `100.69.31.14:8501` **ou** `https://datalab-server.taila180c3.ts.net/` | o link Funnel é a **única exceção** ao pré-requisito de Tailscale abaixo |

Pré-requisito comum: estar na **Tailscale** do time para alcançar
`100.69.31.14` — exceto o painel de negócio (Streamlit), exposto
publicamente via Tailscale Funnel, acessível de qualquer rede sem VPN.
