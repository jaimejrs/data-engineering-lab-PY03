"""
Extração paginada da API REST do Ceará Transparente (contratos).

Endpoint: GET /transparencia/contratos/contratos
Paginação: até 100 registros por página; summary.total_pages indica o total.
Uso: `python -m src.extractors.api_extractor --inicio 2026-01-01 --fim 2026-01-31`
"""

import argparse
import logging
import os
import time
from datetime import date

import requests
from dotenv import load_dotenv

from .storage import write_json_records

load_dotenv()

logger = logging.getLogger(__name__)

API_BASE_URL = os.environ.get(
    "CEARA_TRANSPARENTE_API_URL",
    "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos",
)
REQUEST_TIMEOUT = int(os.environ.get("CEARA_API_TIMEOUT_SECONDS", "30"))
SLEEP_BETWEEN_PAGES = float(os.environ.get("CEARA_API_SLEEP_SECONDS", "1.0"))
MAX_RETRIES = int(os.environ.get("CEARA_API_MAX_RETRIES", "3"))

# Início da carga histórica completa — data mínima de data_assinatura confirmada
# via API em 2026-07-18 (209.010 contratos / 2.091 páginas até essa data).
# Usado apenas como default de CLI; a DAG deve sobrescrever --inicio com o valor
# da Airflow Variable de última extração a partir da segunda execução em diante.
FULL_LOAD_START_DATE = "2022-01-10"


class CearaTransparenteAPIError(RuntimeError):
    """Erro irrecuperável ao consultar a API do Ceará Transparente."""


def _to_api_date(iso_date):
    """Converte 'YYYY-MM-DD' para 'DD/MM/YYYY' — formato exigido pela API real.

    Com data em ISO a API não retorna JSON, retorna um texto de erro pedindo
    para preencher os parâmetros (confirmado em 2026-07-16 contra o servidor real).
    """
    if not iso_date:
        return iso_date
    year, month, day = iso_date.split("-")
    return f"{day}/{month}/{year}"


def _request_page(session, page, data_assinatura_inicio, data_assinatura_fim):
    params = {"page": page}
    if data_assinatura_inicio:
        params["data_assinatura_inicio"] = _to_api_date(data_assinatura_inicio)
    if data_assinatura_fim:
        params["data_assinatura_fim"] = _to_api_date(data_assinatura_fim)

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", SLEEP_BETWEEN_PAGES * attempt))
                logger.warning("Rate limit (429) na página %s — aguardando %.1fs", page, retry_after)
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            wait = SLEEP_BETWEEN_PAGES * attempt
            logger.warning(
                "Falha na página %s (tentativa %s/%s): %s. Aguardando %.1fs",
                page, attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)

    raise CearaTransparenteAPIError(
        f"Falha ao obter página {page} após {MAX_RETRIES} tentativas"
    ) from last_error


def fetch_contratos(data_assinatura_inicio=None, data_assinatura_fim=None, session=None):
    """
    Gera (page, records) para cada página de contratos no intervalo informado.

    Interrompe assim que `page` ultrapassa summary.total_pages — nunca itera
    indefinidamente. Se a API responder sem summary.total_pages, aborta em vez
    de assumir que há mais páginas.
    """
    owns_session = session is None
    session = session or requests.Session()
    try:
        page = 1
        total_pages = None
        while total_pages is None or page <= total_pages:
            payload = _request_page(session, page, data_assinatura_inicio, data_assinatura_fim)

            # A API real usa a chave "sumary" (typo, sem o 2º "m"), confirmado em
            # 2026-07-16. Mantemos o fallback para "summary" caso corrijam o typo.
            summary = payload.get("sumary") or payload.get("summary") or {}
            total_pages = summary.get("total_pages")
            if total_pages is None:
                raise CearaTransparenteAPIError(
                    f"Página {page}: resposta sem sumary.total_pages — abortando para evitar laço infinito"
                )

            records = payload.get("data") or payload.get("results") or []
            logger.info("Página %s/%s: %s registros", page, total_pages, len(records))
            yield page, records

            page += 1
            if page <= total_pages:
                time.sleep(SLEEP_BETWEEN_PAGES)
    finally:
        if owns_session:
            session.close()


def _max_data_assinatura(records):
    """Maior data_assinatura (YYYY-MM-DD) entre os registros, ou None se vazio/ausente."""
    dates = [r["data_assinatura"][:10] for r in records if r.get("data_assinatura")]
    return max(dates) if dates else None


def _partition_records(records, field="data_assinatura"):
    """Agrupa registros em (ano, mes, sub_records) a partir de `field` (ISO 'YYYY-MM-DD...').

    Uma mesma página pode conter registros de meses diferentes, então o
    agrupamento é por registro, não por página inteira. Registros sem o campo
    (não deveria ocorrer na API real) caem no grupo sem partição (ano=None,
    mes=None).
    """
    groups = {}
    order = []
    for record in records:
        value = record.get(field)
        key = (value[:4], value[5:7]) if value and len(value) >= 7 else (None, None)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(record)
    for key in order:
        yield key[0], key[1], groups[key]


def extract_and_save(data_assinatura_inicio=None, data_assinatura_fim=None, run_date=None):
    """
    Executa a extração completa e grava os contratos na Bronze particionados
    por `ano=YYYY/mes=MM` (a partir de data_assinatura), mesmo esquema que a
    Silver usa (seção 4.2 do enunciado) — assim a futura DAG de extração
    incremental só precisa tocar na partição do período corrente.

    Retorna apenas metadados leves (contagens e a maior data_assinatura vista) —
    seguro para XCom do Airflow, nunca os registros em si. `max_data_assinatura`
    é o valor que a DAG deve gravar na Airflow Variable para a próxima extração
    incremental usar como `--inicio` (item 7.1 do enunciado).
    """
    run_date = run_date or date.today().isoformat()
    total_records = 0
    total_pages = 0
    max_data_assinatura = None
    partition_counters = {}

    for page, records in fetch_contratos(data_assinatura_inicio, data_assinatura_fim):
        total_pages = page

        for ano, mes, sub_records in _partition_records(records):
            key = (ano, mes)
            partition_counters[key] = partition_counters.get(key, 0) + 1
            file_index = partition_counters[key]

            if ano and mes:
                relative_path = (
                    f"contratos/ano={ano}/mes={mes}/data_extracao={run_date}/page_{file_index:04d}.json"
                )
            else:
                relative_path = f"contratos/data_extracao={run_date}/page_{file_index:04d}.json"

            write_json_records(relative_path, sub_records)
            total_records += len(sub_records)

        page_max = _max_data_assinatura(records)
        if page_max and (max_data_assinatura is None or page_max > max_data_assinatura):
            max_data_assinatura = page_max

    return {
        "total_pages": total_pages,
        "total_records": total_records,
        "run_date": run_date,
        "max_data_assinatura": max_data_assinatura,
    }


def _parse_args():
    parser = argparse.ArgumentParser(description="Extração de contratos do Ceará Transparente")
    parser.add_argument(
        "--inicio",
        dest="data_assinatura_inicio",
        default=FULL_LOAD_START_DATE,
        help="data_assinatura_inicio (YYYY-MM-DD). Default: início da carga histórica completa.",
    )
    parser.add_argument(
        "--fim",
        dest="data_assinatura_fim",
        default=date.today().isoformat(),
        help="data_assinatura_fim (YYYY-MM-DD). Default: data de hoje.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    result = extract_and_save(args.data_assinatura_inicio, args.data_assinatura_fim)
    logger.info("Extração concluída: %s", result)
