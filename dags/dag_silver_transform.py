"""
DAG 2 — Transformação Silver (Bronze -> tabelas Iceberg no HDFS).

Escopo: orquestra a transformação Bronze -> Silver (normalização de datas,
CNPJ/CPF, dedup e particionamento) logo após a Bronze de uma `data_extracao`
ser validada pela DAG 1.

Execução via **DockerOperator** na imagem `datalab-spark:local` (runtime Spark
comprovado — Java 17 + Spark 3.5.3 + jar do Iceberg baked), rodando o
`silver_job.py` em `spark-submit local[*]`. Optou-se por DockerOperator (em vez
de SparkSubmitOperator client-mode) porque a imagem do Airflow não é um bom
runtime Spark e o client-mode entre containers tinha problemas de rede de
executores — ver docs/06. O job faz `MERGE INTO` nas tabelas Iceberg
(idempotente / dedup entre execuções).

Disparo: por Dataset (`BRONZE_VALIDATED_DATASET`, emitido por `advance_watermark`
da DAG 1). Ao terminar, emite `SILVER_READY_DATASET`, que dispara a DAG 3 (Gold).
`--run-date` vem da Airflow Variable de watermark.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Garante `dags`/`src` importáveis sob o parsing isolado do Airflow.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dags.common import (  # noqa: E402
    BRONZE_VALIDATED_DATASET,
    DBT_DOCKER_NETWORK,
    SILVER_READY_DATASET,
    WATERMARK_VARIABLE,
)

default_args = {
    "owner": "jaime",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

RUN_DATE_TEMPLATE = f"{{{{ var.value.{WATERMARK_VARIABLE} }}}}"

# Diretório do `src` no host (para o bind-mount do container Spark). No servidor,
# /home/dataadm/lakehouse/src; no stack autônomo, ajuste conforme o host.
LAKEHOUSE_SRC_DIR = os.environ.get("LAKEHOUSE_SRC_DIR", "/home/dataadm/lakehouse/src")

# Config do lakehouse repassada ao container Spark (lida pelo spark_session).
SPARK_JOB_ENV = {
    "HIVE_METASTORE_URI": os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"),
    "ICEBERG_WAREHOUSE": os.environ.get("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse"),
    "HDFS_DEFAULT_FS": os.environ.get("HDFS_DEFAULT_FS", "hdfs://namenode:9000"),
    "BRONZE_BASE_PATH": os.environ.get("BRONZE_BASE_PATH", "/bronze"),
    "HADOOP_USER_NAME": os.environ.get("HADOOP_USER_NAME", "root"),
}


@dag(
    dag_id="silver_transform",
    description="DAG 2 — Bronze -> Silver (Iceberg) via Spark (DockerOperator)",
    default_args=default_args,
    schedule=[BRONZE_VALIDATED_DATASET],
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transformacao", "iceberg", "spark", "fase-2"],
)
def silver_transform():
    DockerOperator(
        task_id="transform",
        image="datalab-spark:local",
        # Runtime Spark da imagem; local[*] (sem cluster) -> sem rede de executores.
        entrypoint=["/opt/spark/bin/spark-submit"],
        command=[
            "--driver-memory", "4g",
            "/opt/datalab/src/spark_jobs/silver_job.py",
            "--run-date", RUN_DATE_TEMPLATE,
        ],
        environment=SPARK_JOB_ENV,
        mounts=[Mount(source=LAKEHOUSE_SRC_DIR, target="/opt/datalab/src", type="bind", read_only=True)],
        network_mode=DBT_DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        outlets=[SILVER_READY_DATASET],
    )


silver_transform()
