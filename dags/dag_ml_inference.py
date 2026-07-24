"""
DAG 4 — Inferência de ML (Modelo 1 — anomalias, Modelo 2 — previsão de pagamentos).

Escopo (Fase 3, tarefas 20-24): roda os dois modelos sobre o snapshot mais
recente da Gold, logo após o `dbt build` da DAG 3 terminar.

Tasks:
  - `score_anomalias` — treina o Isolation Forest (`models/anomaly_detection.py`)
    e grava `(id_contrato_origem, ano, score_anomalia)` em
    `iceberg.gold.score_anomalia_contrato` (tarefa 20/24).
  - `prever_pagamentos` — treina os regressores por quantil
    (`models/payment_forecast.py`) e grava a previsão em
    `iceberg.gold.previsao_pagamento_orgao` (tarefa 21).
  - `refresh_fato_contrato` — como `fato_contrato` é `materialized='table'` no
    dbt (recriada do zero a cada build, não incremental), o score gravado por
    `score_anomalias` só aparece nela depois de outro `dbt build`. Em vez de
    esperar o próximo ciclo diário (DAG 3 só dispara de novo com a próxima
    Bronze), esta task roda `dbt build --select fato_contrato` uma vez, aqui
    mesmo, fechando o loop score -> fato na mesma execução (tarefa 24).

Execução: `score_anomalias`/`prever_pagamentos` via `PythonOperator`, direto na
imagem do Airflow (mesmo padrão da DAG 1 — import direto de `models/`, sem
puxar um container extra: scikit-learn/xgboost são leves perto de Spark/dbt,
que sim justificam DockerOperator). `refresh_fato_contrato` reusa o padrão da
DAG 3 (`DockerOperator` na imagem `datalab-dbt`).

Disparo: por Dataset (`GOLD_READY_DATASET`, emitido pela DAG 3). Modelos não
supervisionados/sem rótulo — avaliação e evidência de treino ficam nos
notebooks `notebooks/ml_anomalia_contratos.ipynb` e
`notebooks/ml_previsao_pagamentos.ipynb`, não nesta DAG (que só re-treina e
escora em produção).
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

# Garante `dags`/`models` importáveis sob o parsing isolado do Airflow.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dags.common import DBT_DOCKER_NETWORK, GOLD_READY_DATASET  # noqa: E402

default_args = {
    "owner": "jaime",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ml_inference",
    description="DAG 4 — Modelo 1 (anomalias) + Modelo 2 (previsão de pagamentos)",
    default_args=default_args,
    schedule=[GOLD_READY_DATASET],
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "anomalia", "previsao", "fase-3"],
)
def ml_inference():

    @task
    def score_anomalias():
        from models.anomaly_detection import run

        resultado = run(contamination="auto")
        return {"contratos_escorados": len(resultado)}

    @task
    def prever_pagamentos():
        from models.payment_forecast import run

        resultado = run()
        return {"orgaos_previstos": len(resultado)}

    refresh_fato_contrato = DockerOperator(
        task_id="refresh_fato_contrato",
        image="datalab-dbt:local",
        command="build --select fato_contrato",
        network_mode=DBT_DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    score_anomalias() >> refresh_fato_contrato
    prever_pagamentos()


ml_inference()
