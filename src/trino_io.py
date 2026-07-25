"""Conexão/consulta/escrita compartilhadas com o Trino — usadas pelos módulos de
ML (`models/anomaly_detection.py`, `models/payment_forecast.py`) para ler
Silver/Gold e gravar de volta os resultados dos modelos, e pela DAG 1
(`src/reconciliation.py`) para registrar contagens de auditoria. Vive em `src/`
(não em `models/`) por ser infra genérica de Trino, não específica de ML.

Não há `executemany` no cliente `trino` (DBAPI puro); para os volumes daqui
(milhares a centenas de milhares de linhas por execução, não milhões) um
`INSERT ... VALUES` em lotes é suficiente e evita puxar Spark só para gravar
previsão de modelo.
"""

import logging
import os
from typing import Any

import pandas as pd
import trino
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Linhas por lote no `bulk_insert` — ajustável sem redeploy de código (ex: se um
# modelo crescer muito além de ~200 mil linhas por execução e o tempo de
# gravação sequencial virar gargalo real). Ver docs/03-pendencias-e-melhorias.md.
DEFAULT_CHUNK_SIZE = int(os.environ.get("TRINO_BULK_INSERT_CHUNK_SIZE", "5000"))


def connect(schema: str = "gold") -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", 8080)),
        user=os.environ.get("TRINO_USER", "notebook"),
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        schema=schema,
    )


def query(sql: str, conn: trino.dbapi.Connection | None = None) -> pd.DataFrame:
    """Executa um `SELECT` e devolve o resultado como DataFrame."""
    own_conn = conn is None
    conn = conn or connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        if own_conn:
            conn.close()


def execute(sql: str, conn: trino.dbapi.Connection | None = None) -> None:
    """Executa DDL/DML sem resultado tabular (CREATE/DELETE/INSERT)."""
    own_conn = conn is None
    conn = conn or connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()  # o cliente Trino exige drenar o cursor mesmo sem linhas
    finally:
        if own_conn:
            conn.close()


def _sql_literal(value: Any, cast: str | None = None) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        literal = "NULL"
    elif isinstance(value, bool):
        literal = "true" if value else "false"
    elif isinstance(value, int | float):
        literal = repr(value)
    else:
        literal = "'" + str(value).replace("'", "''") + "'"
    if cast and literal != "NULL":
        return f"CAST({literal} AS {cast})"
    return literal


def bulk_insert(
    table: str,
    df: pd.DataFrame,
    columns: list[str],
    conn: trino.dbapi.Connection | None = None,
    chunk_size: int | None = None,
    casts: dict[str, str] | None = None,
) -> None:
    """`INSERT INTO table (...) VALUES (...), (...), ...` em lotes de `chunk_size` linhas.

    `casts` mapeia coluna -> tipo SQL (ex: `{"scored_at": "TIMESTAMP"}`) para
    colunas cujo literal de texto precisa de cast explícito no Trino.
    `chunk_size` (default: `DEFAULT_CHUNK_SIZE`, controlado por
    `TRINO_BULK_INSERT_CHUNK_SIZE`) — sem `executemany` no driver, cada lote é
    um round-trip; lote maior reduz o número de round-trips (ex: ~215 mil
    linhas em lotes de 500 são ~430 INSERTs sequenciais, minutos de execução;
    em lotes de 5000 caem para ~43). Se isso virar gargalo de verdade (bem
    além de ~200 mil linhas por execução), o próximo passo não é só aumentar o
    lote — é escrever via staging + `INSERT INTO ... SELECT` em vez de
    `VALUES` literal; paralelizar os lotes NÃO é seguro aqui, o Iceberg usa
    controle de concorrência otimista e dois writers concorrentes na mesma
    tabela colidem (visto na prática — ver docs/06-analise-critica.md, item 13).
    """
    if df.empty:
        return
    casts = casts or {}
    chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
    own_conn = conn is None
    conn = conn or connect()
    total_chunks = (len(df) + chunk_size - 1) // chunk_size
    try:
        cur = conn.cursor()
        for i, start in enumerate(range(0, len(df), chunk_size), start=1):
            chunk = df.iloc[start : start + chunk_size]
            rows_sql = ",\n".join(
                "(" + ", ".join(_sql_literal(row[c], casts.get(c)) for c in columns) + ")"
                for _, row in chunk.iterrows()
            )
            cur.execute(f"INSERT INTO {table} ({', '.join(columns)}) VALUES {rows_sql}")
            cur.fetchall()
            logger.info("bulk_insert %s: lote %s/%s (%s linhas) gravado", table, i, total_chunks, len(chunk))
    finally:
        if own_conn:
            conn.close()


def replace_table(
    table: str,
    df: pd.DataFrame,
    columns: list[str],
    ddl: str,
    conn: trino.dbapi.Connection | None = None,
    chunk_size: int | None = None,
    casts: dict[str, str] | None = None,
) -> None:
    """`CREATE TABLE IF NOT EXISTS` (via `ddl`) + `DELETE FROM` (limpa) + `INSERT` do `df`.

    Padrão de "re-score em lote": os modelos aqui não são incrementais (rodam
    sobre o snapshot inteiro da Gold a cada execução), então a tabela de saída é
    sempre substituída por completo, não fundida linha a linha.
    """
    own_conn = conn is None
    conn = conn or connect()
    try:
        execute(ddl, conn=conn)
        execute(f"DELETE FROM {table}", conn=conn)
        bulk_insert(table, df, columns, conn=conn, chunk_size=chunk_size, casts=casts)
    finally:
        if own_conn:
            conn.close()
