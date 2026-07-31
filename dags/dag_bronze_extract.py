"""
DAG 1 — Extração e Carga Bronze.

Escopo (Fase 1, Membro 1): orquestra a extração incremental das duas fontes
(PostgreSQL de origem e API do Ceará Transparente) para a camada Bronze, e
valida schema/completude antes de avançar o watermark incremental.

Tasks: extract_postgres, extract_api -> validate_bronze -> advance_watermark.
"""

import logging
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
from src.reconciliation import record_bronze_counts  # noqa: E402
from src.validators.bronze_validator import BronzeValidationError, validate_bronze  # noqa: E402

DEFAULT_WATERMARK = "2026-01-01"  # usado apenas na primeira execução, sem histórico prévio

# Janela de reprocessamento (dias) subtraída do watermark na extração. O
# filtro é sobre a data de EVENTO (dataemissao/data_assinatura); sem
# lookback, lançamento retroativo na origem nunca seria capturado. O MERGE
# da Silver é idempotente, então reprocessar a sobreposição não duplica.
LOOKBACK_DAYS = int(os.environ.get("BRONZE_LOOKBACK_DAYS", "7"))

# Fontes com watermark de evento próprio — `unidade_gestora` fica de fora,
# não tem coluna de data, é sempre carga cheia.
EVENT_WATERMARK_SOURCES = ("empenhos", "ordem_bancaria_orcamentaria", "contratos")

# As 2 tabelas do Postgres com data são extraídas numa ÚNICA chamada (mesma
# conexão/engine); usam o início mais conservador (mais antigo) das duas
# watermarks — reprocessa um pouco a mais pra tabela já em dia, mas nunca
# perde dado atrasado da outra. Simplificação deliberada: watermark por fonte
# de verdade exigiria separar a extração em 2 chamadas independentes.
POSTGRES_INCREMENTAL_TABLES = ("empenhos", "ordem_bancaria_orcamentaria")


def _event_watermark_variable(source: str) -> str:
    return f"bronze_watermark_evento_{source}"


def _extract_start(source: str) -> str:
    """Data inicial da extração de `source` = watermark de evento da própria
    fonte (maior `dataemissao`/`data_assinatura` já vista nela) − LOOKBACK_DAYS.

    Fallback pro watermark global antigo (`WATERMARK_VARIABLE`) enquanto a
    Variable por fonte ainda não existe, em vez de `DEFAULT_WATERMARK` — evita
    reextrair a carga histórica inteira na primeira execução após o deploy
    desta lógica.
    """
    fallback = Variable.get(WATERMARK_VARIABLE, default_var=DEFAULT_WATERMARK)
    watermark = Variable.get(_event_watermark_variable(source), default_var=fallback)
    try:
        base = datetime.strptime(watermark, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return watermark
    return (base - timedelta(days=LOOKBACK_DAYS)).isoformat()


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
        inicio = min(_extract_start(t) for t in POSTGRES_INCREMENTAL_TABLES)
        return postgres_extractor.extract_and_save(data_inicio=inicio, data_fim=ds, run_date=ds)

    @task
    def extract_api():
        ds = get_current_context()["ds"]
        return api_extractor.extract_and_save(
            data_assinatura_inicio=_extract_start("contratos"), data_assinatura_fim=ds, run_date=ds
        )

    @task
    def validate(postgres_result, api_result):
        ds = get_current_context()["ds"]
        try:
            result = validate_bronze(run_date=ds)
        except BronzeValidationError as exc:
            raise AirflowException(f"Validação da Bronze falhou para data_extracao={ds}: {exc}") from exc

        # Persiste a contagem que a validação já calculou, sem reler a
        # Bronze. Observabilidade — falha aqui não pode derrubar a ingestão.
        log = logging.getLogger(__name__)
        try:
            record_bronze_counts(ds, {source: info["records"] for source, info in result.items()})
            log.info("Reconciliação: contagens de Bronze gravadas para data_extracao=%s", ds)
        except Exception as exc:
            log.warning("Falha ao gravar contagens de reconciliação para data_extracao=%s: %s", ds, exc)

        return result

    @task(outlets=[BRONZE_VALIDATED_DATASET])
    def advance_watermark(validation_result, postgres_result, api_result):
        ds = get_current_context()["ds"]
        # Partição (`data_extracao`) que a DAG 2 (Silver) deve ler — data de
        # processamento, não de evento (watermark de evento é o loop abaixo).
        Variable.set(WATERMARK_VARIABLE, ds)

        # Avança com o maior dataemissao/data_assinatura realmente visto
        # nesta execução, não com `ds`. Fonte sem dado novo não regride.
        for table, max_date in (postgres_result.get("max_dates") or {}).items():
            if table in EVENT_WATERMARK_SOURCES and max_date:
                Variable.set(_event_watermark_variable(table), max_date)
        if api_result.get("max_data_assinatura"):
            Variable.set(_event_watermark_variable("contratos"), api_result["max_data_assinatura"])

        return ds

    postgres_result = extract_postgres()
    api_result = extract_api()
    validation_result = validate(postgres_result, api_result)
    advance_watermark(validation_result, postgres_result, api_result)


bronze_extract()
