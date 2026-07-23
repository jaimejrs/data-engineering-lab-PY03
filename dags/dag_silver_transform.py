"""
DAG 2 — Transformação Silver (Bronze -> tabelas Iceberg no HDFS).

Escopo: orquestra a transformação Bronze -> Silver (normalização de datas,
CNPJ/CPF, dedup e particionamento) logo após a Bronze de uma `data_extracao`
ser validada pela DAG 1.

Arquitetura lakehouse (Spark + Iceberg + HDFS, catálogo Hive Metastore): a
transformação roda como job PySpark (`src/spark_jobs/silver_job.py`) submetido a
um cluster Spark standalone via `SparkSubmitOperator` (client mode). O job grava
tabelas Iceberg `lakehouse.silver.*` e faz `MERGE INTO` pela chave de negócio —
deduplicando **entre execuções**, não só dentro de uma (ver
`documentacao/lakehouse-spark-iceberg.md`).

Disparo: por Dataset (`BRONZE_VALIDATED_DATASET`, emitido por `advance_watermark`
da DAG 1). Ao terminar, emite `SILVER_READY_DATASET`, que dispara a DAG 3 (Gold).
`--run-date` vem da Airflow Variable de watermark (mesma `data_extracao` que a
DAG 1 acabou de validar) — não do `ds` do disparo por Dataset.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dags.common import (
    BRONZE_VALIDATED_DATASET,
    SILVER_READY_DATASET,
    SPARK_EXTRA_JARS,
    SPARK_SUBMIT_CONF,
    WATERMARK_VARIABLE,
)

# run-date = mesma data_extracao que a DAG 1 validou (Airflow Variable de watermark),
# renderizada por Jinja no submit — não o `ds` do disparo por Dataset.
RUN_DATE_TEMPLATE = f"{{{{ var.value.{WATERMARK_VARIABLE} }}}}"

default_args = {
    "owner": "jaime",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="silver_transform",
    description="DAG 2 — Bronze -> Silver (Iceberg) via Spark",
    default_args=default_args,
    schedule=[BRONZE_VALIDATED_DATASET],
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transformacao", "iceberg", "spark", "fase-2"],
)
def silver_transform():
    SparkSubmitOperator(
        task_id="transform",
        application="/opt/airflow/src/spark_jobs/silver_job.py",
        conn_id="spark_default",
        deploy_mode="client",
        name="silver_transform",
        application_args=["--run-date", RUN_DATE_TEMPLATE],
        jars=SPARK_EXTRA_JARS,
        conf=SPARK_SUBMIT_CONF,
        outlets=[SILVER_READY_DATASET],
    )


silver_transform()
