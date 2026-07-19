"""
DAG 1 — Extração e Carga Bronze.

Escopo (Fase 1, Membro 1): orquestra a extração incremental das duas fontes
(PostgreSQL de origem e API do Ceará Transparente) para a camada Bronze, e
valida schema/completude antes de avançar o watermark incremental.

Tasks: extract_postgres, extract_api -> validate_bronze -> advance_watermark.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import get_current_context

# Garante que `src/` seja importável mesmo se o Airflow não tiver o repositório
# inteiro no PYTHONPATH (ex: apenas dags/ montado no container).
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dags.common import BRONZE_VALIDATED_DATASET, WATERMARK_VARIABLE  # noqa: E402
from src.extractors import api_extractor, postgres_extractor  # noqa: E402
from src.validators.bronze_validator import BronzeValidationError, validate_bronze  # noqa: E402

DEFAULT_WATERMARK = "2026-01-01"  # usado apenas na primeira execução, sem histórico prévio

default_args = {
    "owner": "jaime",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bronze_extract",
    description="DAG 1 — extração incremental de PostgreSQL e API para a camada Bronze",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestao", "fase-1"],
)
def bronze_extract():

    @task
    def extract_postgres():
        ds = get_current_context()["ds"]
        watermark = Variable.get(WATERMARK_VARIABLE, default_var=DEFAULT_WATERMARK)
        return postgres_extractor.extract_and_save(data_inicio=watermark, data_fim=ds, run_date=ds)

    @task
    def extract_api():
        ds = get_current_context()["ds"]
        watermark = Variable.get(WATERMARK_VARIABLE, default_var=DEFAULT_WATERMARK)
        return api_extractor.extract_and_save(
            data_assinatura_inicio=watermark, data_assinatura_fim=ds, run_date=ds
        )

    @task
    def validate(postgres_result, api_result):
        ds = get_current_context()["ds"]
        try:
            return validate_bronze(run_date=ds)
        except BronzeValidationError as exc:
            raise AirflowException(f"Validação da Bronze falhou para data_extracao={ds}: {exc}") from exc

    @task(outlets=[BRONZE_VALIDATED_DATASET])
    def advance_watermark(validation_result):
        ds = get_current_context()["ds"]
        Variable.set(WATERMARK_VARIABLE, ds)
        return ds

    postgres_result = extract_postgres()
    api_result = extract_api()
    validation_result = validate(postgres_result, api_result)
    advance_watermark(validation_result)


bronze_extract()
