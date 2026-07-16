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


def extract_and_save(data_assinatura_inicio=None, data_assinatura_fim=None, run_date=None):
    """
    Executa a extração completa e grava cada página como JSON na Bronze.

    Retorna apenas metadados leves (contagens) — seguro para XCom do Airflow,
    nunca os registros em si.
    """
    run_date = run_date or date.today().isoformat()
    total_records = 0
    total_pages = 0

    for page, records in fetch_contratos(data_assinatura_inicio, data_assinatura_fim):
        relative_path = f"contratos/data_extracao={run_date}/page_{page:04d}.json"
        write_json_records(relative_path, records)
        total_records += len(records)
        total_pages = page

    return {"total_pages": total_pages, "total_records": total_records, "run_date": run_date}


def _parse_args():
    parser = argparse.ArgumentParser(description="Extração de contratos do Ceará Transparente")
    parser.add_argument("--inicio", dest="data_assinatura_inicio", help="data_assinatura_inicio (YYYY-MM-DD)")
    parser.add_argument("--fim", dest="data_assinatura_fim", help="data_assinatura_fim (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    result = extract_and_save(args.data_assinatura_inicio, args.data_assinatura_fim)
    logger.info("Extração concluída: %s", result)
