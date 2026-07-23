"""Constantes compartilhadas entre `dag_bronze_extract.py` e `dag_silver_transform.py`.

Isoladas num módulo à parte, sem nenhum `@dag`/`DAG(...)`: o `DagBag` do Airflow
executa qualquer módulo importado por um arquivo de DAG e recolhe objetos DAG
encontrados nele também — importar `dag_bronze_extract.py` diretamente fazia o
scheduler registrar o dag_id `bronze_extract` duas vezes (uma vindo do próprio
arquivo, outra "encontrada" via import em `dag_silver_transform.py`), gerando
`AirflowDagDuplicatedIdException` (confirmado rodando no Airflow real do
Datalab em 19/07/2026).
"""

import os

from airflow.datasets import Dataset

WATERMARK_VARIABLE = "bronze_last_data_extracao"

# Emitido quando a Bronze de uma data_extracao é validada — a DAG 2
# (dag_silver_transform) usa isso como schedule em vez de horário fixo.
BRONZE_VALIDATED_DATASET = Dataset("bronze://validated")

# Emitido quando a Silver termina de transformar uma data_extracao — a DAG 3
# (dag_gold_load) usa isso como schedule, mesmo padrão da DAG 1 -> DAG 2.
SILVER_READY_DATASET = Dataset("silver://ready")

# --- Submit Spark (DAGs 2 e 3, SparkSubmitOperator, cluster standalone) ---

# Jars Iceberg + JDBC Postgres embutidos na imagem do Airflow (docker/airflow),
# passados ao spark-submit em client mode (driver roda no scheduler).
SPARK_EXTRA_JARS = (
    "/opt/spark-extra-jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
    "/opt/spark-extra-jars/postgresql-42.7.4.jar"
)

# Conf comum: em client mode o driver roda no container do scheduler; os
# executores do cluster precisam reconectar ao driver por esse hostname (alias
# de rede do scheduler). Parametrizável por env para servir repo autônomo
# (`airflow-scheduler`) e servidor (`datalab_airflow_scheduler`).
SPARK_SUBMIT_CONF = {
    "spark.driver.host": os.environ.get("SPARK_DRIVER_HOST", "airflow-scheduler"),
    "spark.driver.bindAddress": "0.0.0.0",
}

# Rede Docker onde o DockerOperator (DAG 3, dbt) roda — `datalab_net` no stack
# autônomo, `dataadm_default` no servidor.
DBT_DOCKER_NETWORK = os.environ.get("DBT_DOCKER_NETWORK", "datalab_net")
