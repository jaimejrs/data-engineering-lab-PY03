"""
DAG 3 — Carga Gold (Data Warehouse dimensional).

Escopo (Fase 2, Membro 1): orquestra a carga do DW (`sql/ddl_dw.sql`) a
partir do histórico acumulado da Silver, logo após a DAG 2 terminar de
transformar uma `data_extracao`. Ver `src/loaders/dw_loader.py` pela lógica
de cada dimensão/fato (tarefas 15/16).

Disparo: por Dataset (`SILVER_READY_DATASET`, emitido pela task `transform`
da DAG 2), mesmo padrão da DAG 1 -> DAG 2.

Tasks separadas (pedido explícito do checklist, diferente da DAG 2 que usa
uma task única): `apply_ddl` -> uma task por dimensão -> uma task por fato.
Cada task lê a Silver/DW de forma independente (sem passar DataFrames grandes
via XCom) — as tasks de fato só dependem das de dimensão via trigger order,
e releem as dimensões já commitadas direto do Postgres.
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dags.common import SILVER_READY_DATASET  # noqa: E402
from src.loaders import dw_loader  # noqa: E402
from src.transformers.enrichment import enrich_with_unidade_gestora  # noqa: E402
from src.transformers.silver_storage import read_source  # noqa: E402

default_args = {
    "owner": "jaime",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="gold_load",
    description="DAG 3 — carga do DW dimensional (Gold) a partir da Silver",
    default_args=default_args,
    schedule=[SILVER_READY_DATASET],
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dw", "fase-2"],
)
def gold_load():

    @task
    def apply_ddl():
        dw_loader.apply_ddl()

    @task
    def load_dim_credor():
        engine = dw_loader._get_engine()
        contratos = read_source("contratos")
        return len(dw_loader.load_dim_credor(engine, contratos))

    @task
    def load_dim_orgao():
        engine = dw_loader._get_engine()
        unidade_gestora = read_source("unidade_gestora")
        return len(dw_loader.load_dim_orgao(engine, unidade_gestora))

    @task
    def load_dim_modalidade():
        engine = dw_loader._get_engine()
        contratos = read_source("contratos")
        return len(dw_loader.load_dim_modalidade(engine, contratos))

    @task
    def load_dim_tempo():
        engine = dw_loader._get_engine()
        contratos = read_source("contratos")
        empenhos = read_source("empenhos")
        return len(dw_loader.load_dim_tempo(engine, [
            contratos.get("data_assinatura"),
            empenhos.get("dataemissao"),
        ]))

    @task
    def load_fato_contrato(_dim_credor, _dim_orgao, _dim_modalidade, _dim_tempo):
        # Dimensões já commitadas pelas tasks anteriores -- DataFrame vazio
        # nas funções load_dim_* cai no atalho "só busca o que já existe no
        # Postgres" (ver dw_loader.py), evita reler/reprocessar e evita passar
        # DataFrames grandes via XCom entre tasks.
        engine = dw_loader._get_engine()
        empty = pd.DataFrame()
        contratos = read_source("contratos")
        dim_credor = dw_loader.load_dim_credor(engine, empty)
        dim_orgao = dw_loader.load_dim_orgao(engine, empty)
        dim_modalidade = dw_loader.load_dim_modalidade(engine, empty)
        dim_tempo = dw_loader.load_dim_tempo(engine, [])
        return dw_loader.load_fato_contrato(engine, contratos, dim_credor, dim_orgao, dim_modalidade, dim_tempo)

    @task
    def load_fato_empenho(_dim_orgao, _dim_tempo):
        engine = dw_loader._get_engine()
        empty = pd.DataFrame()
        empenhos = read_source("empenhos")
        unidade_gestora = read_source("unidade_gestora")
        dim_orgao = dw_loader.load_dim_orgao(engine, empty)
        dim_tempo = dw_loader.load_dim_tempo(engine, [])
        empenhos_enriched = enrich_with_unidade_gestora(empenhos, unidade_gestora) if not empenhos.empty else empenhos
        return dw_loader.load_fato_empenho(engine, empenhos_enriched, dim_orgao, dim_tempo)

    ddl = apply_ddl()
    dim_credor = load_dim_credor()
    dim_orgao = load_dim_orgao()
    dim_modalidade = load_dim_modalidade()
    dim_tempo = load_dim_tempo()
    ddl >> [dim_credor, dim_orgao, dim_modalidade, dim_tempo]

    load_fato_contrato(dim_credor, dim_orgao, dim_modalidade, dim_tempo)
    load_fato_empenho(dim_orgao, dim_tempo)


gold_load()
