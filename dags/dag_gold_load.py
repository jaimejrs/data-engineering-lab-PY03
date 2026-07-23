"""
DAG 3 — Carga Gold (Data Warehouse dimensional) via dbt-trino sobre Iceberg.

Escopo: orquestra a construção da Gold a partir da Silver, logo após a DAG 2
transformar uma `data_extracao`.

Arquitetura lakehouse puro: a Gold é construída de forma **declarativa** por um
projeto **dbt-trino** (`dbt/`), que lê as tabelas `iceberg.silver.*` via Trino e
materializa `iceberg.gold.*` (tabelas Iceberg no HDFS) — dims/fatos + testes. O
Trino serve as consultas. Ver `documentacao/gold-dbt-trino.md`.

Execução: `DockerOperator` roda a imagem `datalab-dbt:local` (`dbt build`) na rede
do compose (`datalab_net`), onde resolve o host `trino`. Usa o projeto embutido na
imagem (rebuild da imagem `dbt` para atualizar os modelos). Requer o socket do
Docker montado no scheduler (ver docker-compose.yml). Alternativa manual, com o
projeto vivo: `docker compose run --rm dbt build`.

Substitui a carga imperativa anterior (`gold_job.py`/`dw_loader.py`, agora legado).
Disparo: por Dataset (`SILVER_READY_DATASET`).
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

from dags.common import DBT_DOCKER_NETWORK, SILVER_READY_DATASET

default_args = {
    "owner": "jaime",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="gold_load",
    description="DAG 3 — Gold declarativa (dbt-trino) materializada em Iceberg",
    default_args=default_args,
    schedule=[SILVER_READY_DATASET],
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dbt", "trino", "iceberg", "fase-2"],
)
def gold_load():
    DockerOperator(
        task_id="dbt_build",
        image="datalab-dbt:local",
        # ENTRYPOINT da imagem é `dbt`; o comando abaixo vira `dbt build`.
        # DBT_PROFILES_DIR=/dbt e WORKDIR=/dbt já vêm da imagem.
        command="build",
        network_mode=DBT_DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )


gold_load()
