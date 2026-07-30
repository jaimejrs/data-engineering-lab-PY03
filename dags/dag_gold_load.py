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
Disparo: por Dataset (`SILVER_READY_DATASET`). Ao terminar, emite
`GOLD_READY_DATASET`, que dispara a DAG 4 (`ml_inference` — Modelo 1 + Modelo 2).

`track_dbt_test_health` (auditoria de rigor científico, 30/07/2026): roda logo
após `dbt_build`, reimplementando as condições dos testes WARN (que nunca
bloqueiam o build) e gravando contagem+timestamp em
`iceberg.audit.dbt_test_warnings_historico` — ver `src/dbt_test_health.py`
pro motivo de reimplementar em vez de ler os artefatos do dbt. Não faz parte
da cadeia `outlets=[GOLD_READY_DATASET]` (isso continua disparado só por
`dbt_build`, sem esperar essa task de auditoria).
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

# Garante `dags`/`src` importáveis sob o parsing isolado do Airflow.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dags.common import DBT_DOCKER_NETWORK, GOLD_READY_DATASET, SILVER_READY_DATASET  # noqa: E402

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
    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="datalab-dbt:local",
        # ENTRYPOINT da imagem é `dbt`; o comando abaixo vira `dbt build`.
        # DBT_PROFILES_DIR=/dbt e WORKDIR=/dbt já vêm da imagem.
        command="build",
        network_mode=DBT_DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        outlets=[GOLD_READY_DATASET],
    )

    @task
    def track_dbt_test_health():
        from src.dbt_test_health import run

        return run(persist=True)

    dbt_build >> track_dbt_test_health()


gold_load()
