"""
Extração bruta das tabelas de origem do PostgreSQL para a camada Bronze.

Escopo do Membro 3 (Fase 1): empenhos, ordem_bancaria_orcamentaria, unidade_gestora.
Uso: `python -m src.extractors.postgres_extractor --inicio 2026-01-01 --fim 2026-01-31`

Colunas de data confirmadas contra o schema real do Postgres de origem em
2026-07-16. `dataemissao` é TEXT no formato 'YYYY-MM-DD HH:MM:SS.mmm' — a
comparação lexicográfica com strings 'YYYY-MM-DD' funciona pois o prefixo é
ISO 8601. Nenhuma das tabelas do escopo tem PK declarada no banco.
"""

import argparse
import logging
import os
from datetime import date

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from .storage import write_json_records

load_dotenv()

logger = logging.getLogger(__name__)

SOURCE_DB_URL = os.environ.get(
    "SOURCE_POSTGRES_URL",
    "postgresql://postgres:postgres@localhost:5432/ceara_transparente",
)

# Máximo de linhas por arquivo JSON gravado na Bronze — evita acumular a
# tabela inteira em memória e gravar um único arquivo enorme (empenhos e
# ordem_bancaria_orcamentaria têm centenas de milhares/milhões de linhas
# no histórico completo).
CHUNK_SIZE = int(os.environ.get("POSTGRES_EXTRACT_CHUNK_SIZE", "20000"))

# Tabela -> coluna de data usada para extração incremental (None = tabela de
# referência, extraída por completo a cada execução).
TABLE_DATE_COLUMNS = {
    "empenhos": "dataemissao",
    "ordem_bancaria_orcamentaria": "dataemissao",
    "unidade_gestora": None,
}


def _build_query(table, date_column, data_inicio, data_fim):
    query = f"SELECT * FROM {table}"
    params = {}
    if date_column and (data_inicio or data_fim):
        conditions = []
        if data_inicio:
            conditions.append(f"{date_column} >= :data_inicio")
            params["data_inicio"] = data_inicio
        if data_fim:
            conditions.append(f"{date_column} <= :data_fim")
            params["data_fim"] = data_fim
        query += " WHERE " + " AND ".join(conditions)
    return text(query), params


def extract_table(table, data_inicio=None, data_fim=None, engine=None):
    """Extrai uma tabela do Postgres de origem, filtrando por data quando aplicável."""
    if table not in TABLE_DATE_COLUMNS:
        raise ValueError(f"Tabela '{table}' fora do escopo (esperado: {list(TABLE_DATE_COLUMNS)})")

    owns_engine = engine is None
    engine = engine or create_engine(SOURCE_DB_URL)
    try:
        date_column = TABLE_DATE_COLUMNS[table]
        query, params = _build_query(table, date_column, data_inicio, data_fim)
        df = pd.read_sql(query, engine, params=params)
        logger.info("Extraídos %s registros de '%s'", len(df), table)
        return df
    finally:
        if owns_engine:
            engine.dispose()


def extract_table_chunks(table, data_inicio=None, data_fim=None, engine=None, chunksize=CHUNK_SIZE):
    """Gera DataFrames de até `chunksize` linhas para uma tabela, filtrando por data quando aplicável.

    Usa `pandas.read_sql(..., chunksize=...)`, que busca do cursor em blocos em
    vez de carregar a tabela inteira em memória de uma vez.
    """
    if table not in TABLE_DATE_COLUMNS:
        raise ValueError(f"Tabela '{table}' fora do escopo (esperado: {list(TABLE_DATE_COLUMNS)})")

    owns_engine = engine is None
    engine = engine or create_engine(SOURCE_DB_URL)
    try:
        date_column = TABLE_DATE_COLUMNS[table]
        query, params = _build_query(table, date_column, data_inicio, data_fim)
        for chunk in pd.read_sql(query, engine, params=params, chunksize=chunksize):
            yield chunk
    finally:
        if owns_engine:
            engine.dispose()


def extract_and_save(data_inicio=None, data_fim=None, run_date=None, engine=None):
    """
    Extrai todas as tabelas do escopo e grava cada bloco de até CHUNK_SIZE
    linhas como um JSON separado na Bronze (`chunk_0001.json`, `chunk_0002.json`, ...).

    Retorna apenas contagens por tabela — seguro para XCom do Airflow, nunca
    os DataFrames em si.
    """
    run_date = run_date or date.today().isoformat()
    owns_engine = engine is None
    engine = engine or create_engine(SOURCE_DB_URL)
    counts = {}
    try:
        for table in TABLE_DATE_COLUMNS:
            total_records = 0
            chunk_index = 0
            for chunk_index, chunk in enumerate(
                extract_table_chunks(table, data_inicio, data_fim, engine=engine), start=1
            ):
                records = chunk.to_dict(orient="records")
                relative_path = f"{table}/data_extracao={run_date}/chunk_{chunk_index:04d}.json"
                write_json_records(relative_path, records)
                total_records += len(records)

            if chunk_index == 0:
                # Nenhum chunk veio (0 linhas no filtro) — grava um marcador vazio
                # para deixar explícito que a extração rodou e não encontrou dados.
                write_json_records(f"{table}/data_extracao={run_date}/chunk_0001.json", [])

            counts[table] = total_records
    finally:
        if owns_engine:
            engine.dispose()
    return {"run_date": run_date, "counts": counts}


def _parse_args():
    parser = argparse.ArgumentParser(description="Extração bruta do PostgreSQL para a Bronze")
    parser.add_argument("--inicio", dest="data_inicio", help="Data inicial do filtro (YYYY-MM-DD)")
    parser.add_argument("--fim", dest="data_fim", help="Data final do filtro (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    result = extract_and_save(args.data_inicio, args.data_fim)
    logger.info("Extração concluída: %s", result)
