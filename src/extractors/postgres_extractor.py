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

# Início da carga histórica completa — dataemissao mínima confirmada por
# consulta direta ao Postgres em 2026-07-18 (empenhos: 2022-01-10, OBO:
# 2022-01-14; arredondado para baixo). Usado apenas como default de CLI; a DAG
# deve sobrescrever --inicio com o valor da Airflow Variable de última
# extração a partir da segunda execução em diante.
FULL_LOAD_START_DATE = "2022-01-10"


def _create_engine():
    """Cria a engine com cursor server-side habilitado (`stream_results`).

    Sem isso, `pd.read_sql(..., chunksize=...)` só faz o corte em blocos do
    lado do cliente — o Postgres tenta montar o resultado inteiro da query
    antes de devolver qualquer linha, o que estoura memória no servidor para
    tabelas grandes (~1,3M linhas em empenhos/ordem_bancaria_orcamentaria no
    histórico completo). `stream_results=True` faz o psycopg2 usar um cursor
    nomeado, e o servidor manda os dados em blocos de verdade.
    """
    return create_engine(SOURCE_DB_URL, execution_options={"stream_results": True})


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
    engine = engine or _create_engine()
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
    engine = engine or _create_engine()
    try:
        date_column = TABLE_DATE_COLUMNS[table]
        query, params = _build_query(table, date_column, data_inicio, data_fim)
        for chunk in pd.read_sql(query, engine, params=params, chunksize=chunksize):
            yield chunk
    finally:
        if owns_engine:
            engine.dispose()


def _partition_by_month(chunk, date_column):
    """Agrupa um DataFrame em (ano, mes, sub_df) a partir de `date_column`.

    `date_column` é TEXT 'YYYY-MM-DD HH:MM:SS.mmm' — os 7 primeiros caracteres
    já dão 'YYYY-MM'. Se a coluna não existir no DataFrame (tabela sem data,
    ex: unidade_gestora) ou vier nula, devolve o chunk inteiro sem partição
    (ano=None, mes=None), preservando o layout antigo nesses casos.
    """
    if not date_column or date_column not in chunk.columns or chunk.empty:
        yield None, None, chunk
        return

    ano_mes = chunk[date_column].str.slice(0, 7)
    for periodo, sub_df in chunk.groupby(ano_mes, dropna=False):
        if pd.isna(periodo):
            yield None, None, sub_df
            continue
        ano, mes = periodo.split("-")
        yield ano, mes, sub_df


def extract_and_save(data_inicio=None, data_fim=None, run_date=None, engine=None):
    """
    Extrai todas as tabelas do escopo e grava os dados na Bronze particionados
    por `ano=YYYY/mes=MM` (a partir da coluna de data de cada tabela), mesmo
    esquema que a Silver usa (seção 4.2 do enunciado) — assim a futura DAG de
    extração incremental só precisa tocar na partição do período corrente.
    Tabelas sem coluna de data (ex: unidade_gestora) ficam sem essa partição.

    Retorna apenas contagens e a maior data processada por tabela — seguro para
    XCom do Airflow, nunca os DataFrames em si. `max_dates` é o valor que a DAG
    deve gravar na Airflow Variable para a próxima extração incremental usar
    como `--inicio` (item 7.1 do enunciado).
    """
    run_date = run_date or date.today().isoformat()
    owns_engine = engine is None
    engine = engine or _create_engine()
    counts = {}
    max_dates = {}
    try:
        for table, date_column in TABLE_DATE_COLUMNS.items():
            total_records = 0
            table_max_date = None
            wrote_any = False
            partition_counters = {}

            for chunk in extract_table_chunks(table, data_inicio, data_fim, engine=engine):
                for ano, mes, sub_df in _partition_by_month(chunk, date_column):
                    if sub_df.empty:
                        continue
                    wrote_any = True
                    key = (ano, mes)
                    partition_counters[key] = partition_counters.get(key, 0) + 1
                    file_index = partition_counters[key]

                    if ano and mes:
                        relative_path = (
                            f"{table}/ano={ano}/mes={mes}/data_extracao={run_date}/"
                            f"chunk_{file_index:04d}.json"
                        )
                    else:
                        relative_path = f"{table}/data_extracao={run_date}/chunk_{file_index:04d}.json"

                    records = sub_df.to_dict(orient="records")
                    write_json_records(relative_path, records)
                    total_records += len(records)

                    if date_column and date_column in sub_df.columns:
                        chunk_max = sub_df[date_column].max()[:10]
                        if table_max_date is None or chunk_max > table_max_date:
                            table_max_date = chunk_max

            if not wrote_any:
                # Nenhuma linha veio (0 linhas no filtro) — grava um marcador vazio
                # para deixar explícito que a extração rodou e não encontrou dados.
                write_json_records(f"{table}/data_extracao={run_date}/chunk_0001.json", [])

            counts[table] = total_records
            max_dates[table] = table_max_date
    finally:
        if owns_engine:
            engine.dispose()
    return {"run_date": run_date, "counts": counts, "max_dates": max_dates}


def _parse_args():
    parser = argparse.ArgumentParser(description="Extração bruta do PostgreSQL para a Bronze")
    parser.add_argument(
        "--inicio",
        dest="data_inicio",
        default=FULL_LOAD_START_DATE,
        help="Data inicial do filtro (YYYY-MM-DD). Default: início da carga histórica completa.",
    )
    parser.add_argument(
        "--fim",
        dest="data_fim",
        default=date.today().isoformat(),
        help="Data final do filtro (YYYY-MM-DD). Default: data de hoje.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    result = extract_and_save(args.data_inicio, args.data_fim)
    logger.info("Extração concluída: %s", result)
