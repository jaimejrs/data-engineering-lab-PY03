"""Conexão e consulta ao Trino (camada Gold/ml).

Reimplementa (deliberadamente, não importa) o mesmo padrão de
`src/trino_io.py::connect`/`query` do pipeline principal — este painel é
mantido portátil (roda sem o pacote `src/` no path), então duplica a lógica
de conexão em vez de acoplar aos internals do projeto. Ver documentacao/
mapa-do-codigo.md.
"""

import decimal

import pandas as pd
import trino
from config import TRINO_CATALOG, TRINO_HOST, TRINO_HTTP_SCHEME, TRINO_PORT, TRINO_USER

import streamlit as st


def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema="gold",
        http_scheme=TRINO_HTTP_SCHEME,
    )


@st.cache_data(ttl=300, show_spinner="Consultando o Trino...")
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    # O cliente do Trino retorna colunas DECIMAL como decimal.Decimal.
    # Convertemos para float para evitar erros ao misturar com float/NaN
    # em operações posteriores (merge, fillna, subtração, etc).
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
            df[col] = df[col].astype(float)

    return df
