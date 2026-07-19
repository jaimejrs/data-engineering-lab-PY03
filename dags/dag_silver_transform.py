"""
DAG 2 — Transformação Silver.

Escopo (Fase 2, Membro 1): orquestra a transformação Bronze -> Silver
(normalização de datas, CNPJ/CPF e deduplicação — ver
`src/transformers/silver_transformer.py`) logo após a Bronze de uma
`data_extracao` ser validada pela DAG 1.

Disparo: por Dataset (`BRONZE_VALIDATED_DATASET`, emitido pela task
`advance_watermark` da DAG 1) em vez de horário fixo — evita rodar a Silver
antes da Bronze do dia estar pronta e validada, sem precisar de sensor externo.

Tasks: transform (única task; internamente processa as 4 fontes da Bronze).

Nota (19/07/2026, Jaime): a lógica de normalização/dedup em
`silver_transformer.py` é implementação inicial de apoio, a cargo do Carlos
(tarefas 13/14) revisar/validar — ver `docs/checklist.md`.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dags.common import BRONZE_VALIDATED_DATASET, WATERMARK_VARIABLE  # noqa: E402
from src.transformers.silver_transformer import transform_bronze_to_silver  # noqa: E402

default_args = {
    "owner": "jaime",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="silver_transform",
    description="DAG 2 — transformação Bronze -> Silver (normalização, dedup)",
    default_args=default_args,
    schedule=[BRONZE_VALIDATED_DATASET],
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transformacao", "fase-2"],
)
def silver_transform():

    @task
    def transform():
        # Mesmo valor de data_extracao que a DAG 1 acabou de validar e gravar
        # em advance_watermark — não usa `ds` do próprio disparo por Dataset,
        # que não corresponde à data_extracao da Bronze de origem.
        run_date = Variable.get(WATERMARK_VARIABLE)
        return transform_bronze_to_silver(run_date=run_date)

    transform()


silver_transform()
