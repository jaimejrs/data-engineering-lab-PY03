"""
Carga do Data Warehouse (Gold) — dimensões e fatos, a partir da Silver.

Escopo (Fase 2, tarefas 15/16): aplica `sql/ddl_dw.sql` (idempotente) e faz a
carga incremental por upsert das dimensões (`dim_credor`, `dim_orgao`,
`dim_modalidade`, `dim_tempo`) e dos fatos (`fato_contrato`, `fato_empenho`),
lendo o histórico acumulado da Silver (não só um `data_extracao`).

Insere em lote via `psycopg2.extras.execute_values` -- histórico completo de
contratos passa de 245 mil linhas; um `INSERT` por linha (o que o
`sqlalchemy.Connection.execute` com uma lista de dicts faz por padrão)
seria inviável nesse volume.

Limitação conhecida: `dim_credor` é SCD2 só no schema (`valido_de`/`valido_ate`/
`versao_atual`) -- o loader hoje só insere uma versão nova quando não existe
nenhuma "atual" pro mesmo `cnpj_cpf`; não detecta troca de razão social pra
abrir uma nova versão automaticamente.
"""

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

from src.transformers.enrichment import enrich_with_unidade_gestora
from src.transformers.silver_storage import read_source

load_dotenv()

logger = logging.getLogger(__name__)

DW_POSTGRES_URL = os.environ.get(
    "DW_POSTGRES_URL",
    "postgresql://dw_user:dw_password@localhost:5434/ceara_dw",
)
DDL_PATH = Path(__file__).resolve().parent.parent.parent / "sql" / "ddl_dw.sql"


def _get_engine():
    return create_engine(DW_POSTGRES_URL)


def apply_ddl(engine=None):
    """Aplica `sql/ddl_dw.sql` -- idempotente (`CREATE ... IF NOT EXISTS`)."""
    engine = engine or _get_engine()
    ddl = DDL_PATH.read_text(encoding="utf-8")
    raw = engine.raw_connection()
    try:
        cursor = raw.cursor()
        cursor.execute(ddl)
        raw.commit()
    finally:
        raw.close()
    logger.info("DDL aplicado em %s", DDL_PATH)


def _clean(value):
    """NaN/NaT -> None. `pd.isna` em escalar simples nunca levanta aqui (sem listas/arrays)."""
    return None if pd.isna(value) else value


def _bulk_upsert(engine, table, columns, df, conflict_cols, update_cols=None, conflict_where=None):
    """Insere `df` em lote (`execute_values`) com upsert por `conflict_cols`.

    `update_cols=None` -> `DO NOTHING` (dimensões: não sobrescreve o que já
    existe). `update_cols=[...]` -> `DO UPDATE SET col = EXCLUDED.col` pros
    fatos, recarregáveis de forma idempotente pela chave de negócio.
    """
    if df.empty:
        return

    rows = [tuple(_clean(v) for v in record) for record in df[columns].itertuples(index=False, name=None)]
    collist = ", ".join(columns)
    if update_cols:
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        conflict_clause = f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {update_clause}"
    else:
        where_sql = f" WHERE {conflict_where}" if conflict_where else ""
        conflict_clause = f"ON CONFLICT ({', '.join(conflict_cols)}){where_sql} DO NOTHING"
    sql = f"INSERT INTO {table} ({collist}) VALUES %s {conflict_clause}"

    raw = engine.raw_connection()
    try:
        cursor = raw.cursor()
        execute_values(cursor, sql, rows, page_size=5000)
        raw.commit()
    finally:
        raw.close()
    logger.info("Upsert em %s: %s linha(s)", table, len(rows))


def _upsert_and_fetch(engine, table, unique_cols, rows_df, conflict_where=None):
    """Upsert (dedup pela chave única, ignora conflito) e devolve a tabela completa."""
    where_sql = f" WHERE {conflict_where}" if conflict_where else ""
    if not rows_df.empty:
        rows_df = rows_df.drop_duplicates(subset=unique_cols)
        _bulk_upsert(engine, table, list(rows_df.columns), rows_df, unique_cols, conflict_where=conflict_where)

    return pd.read_sql(f"SELECT * FROM {table}{where_sql}", engine)


def _to_year(iso_date: pd.Series) -> pd.Series:
    return pd.to_numeric(iso_date.str.slice(0, 4), errors="coerce")


def load_dim_credor(engine, contratos: pd.DataFrame) -> pd.DataFrame:
    if contratos.empty:
        return pd.read_sql("SELECT * FROM dw.dim_credor WHERE versao_atual", engine)

    rows = contratos[["cnpj_cpf_normalizado", "descricao_nome_credor", "tipo_credor", "infringement_status"]].copy()
    rows = rows[rows["cnpj_cpf_normalizado"].astype(bool)]
    rows = rows.rename(columns={
        "cnpj_cpf_normalizado": "cnpj_cpf",
        "descricao_nome_credor": "nome",
        "tipo_credor": "tipo",
    })
    rows["historico_infringement"] = rows["infringement_status"].fillna(0).astype(float) > 0
    rows = rows.drop(columns=["infringement_status"])
    return _upsert_and_fetch(engine, "dw.dim_credor", ["cnpj_cpf"], rows, conflict_where="versao_atual")


def load_dim_orgao(engine, unidade_gestora: pd.DataFrame) -> pd.DataFrame:
    if unidade_gestora.empty:
        return pd.read_sql("SELECT * FROM dw.dim_orgao", engine)

    rename = {
        "codigo": "codigo", "ano": "ano", "titulo": "nome", "sigla": "sigla", "cnpj": "cnpj",
        "tipoadministracao": "tipo_administracao", "tipoug": "tipo_ug",
        "codigopoder": "codigo_poder", "nomepoder": "nome_poder",
        "codigouf": "codigo_uf", "nomemunicipio": "nome_municipio",
    }
    present = [c for c in rename if c in unidade_gestora.columns]
    rows = unidade_gestora[present].rename(columns=rename)
    return _upsert_and_fetch(engine, "dw.dim_orgao", ["codigo", "ano"], rows)


def load_dim_modalidade(engine, contratos: pd.DataFrame) -> pd.DataFrame:
    if contratos.empty or "descricao_modalidade" not in contratos.columns:
        return pd.read_sql("SELECT * FROM dw.dim_modalidade", engine)

    rows = contratos[["descricao_modalidade"]].dropna().drop_duplicates()
    rows = rows[rows["descricao_modalidade"].astype(bool)]
    return _upsert_and_fetch(engine, "dw.dim_modalidade", ["descricao_modalidade"], rows)


def load_dim_tempo(engine, date_series_list) -> pd.DataFrame:
    series = [s for s in date_series_list if s is not None]
    dates = pd.concat(series, ignore_index=True) if series else pd.Series(dtype=str)
    dates = dates.dropna()
    dates = dates[dates.astype(bool)].drop_duplicates()
    if dates.empty:
        return pd.read_sql("SELECT * FROM dw.dim_tempo", engine)

    parsed = pd.to_datetime(dates, errors="coerce")
    rows = pd.DataFrame({"data": parsed.dt.date})
    rows = rows.dropna(subset=["data"]).drop_duplicates()
    rows["ano"] = pd.to_datetime(rows["data"]).dt.year
    rows["trimestre"] = pd.to_datetime(rows["data"]).dt.quarter
    rows["mes"] = pd.to_datetime(rows["data"]).dt.month
    rows["dia_semana"] = pd.to_datetime(rows["data"]).dt.dayofweek
    return _upsert_and_fetch(engine, "dw.dim_tempo", ["data"], rows)


def load_fato_contrato(engine, contratos: pd.DataFrame, dim_credor, dim_orgao, dim_modalidade, dim_tempo):
    if contratos.empty:
        return 0

    df = contratos.copy()
    df["ano"] = _to_year(df["data_assinatura"].fillna(""))
    df = df.dropna(subset=["ano", "id"])
    df["ano"] = df["ano"].astype(int)
    # Silver só dedupa dentro de uma mesma execução (ver limitação documentada
    # em dicionario-dados.md/checklist) -- janelas incrementais sobrepostas
    # entre execuções diferentes fazem o mesmo "id" aparecer mais de uma vez
    # no histórico consolidado. Sem isso, ON CONFLICT DO UPDATE falha com
    # CardinalityViolation ao tentar tocar a mesma linha duas vezes no lote.
    df = df.drop_duplicates(subset=["id"], keep="last")

    credor_map = dim_credor.set_index("cnpj_cpf")["sk_credor"].to_dict()
    orgao_map = dim_orgao.set_index(["codigo", "ano"])["sk_orgao"].to_dict()
    modalidade_map = dim_modalidade.set_index("descricao_modalidade")["sk_modalidade"].to_dict()
    tempo_map = dim_tempo.assign(data=dim_tempo["data"].astype(str)).set_index("data")["sk_tempo"].to_dict()

    fato = pd.DataFrame({
        "ano": df["ano"],
        "id_contrato_origem": df["id"].astype(str),
        "sk_credor": df["cnpj_cpf_normalizado"].map(credor_map),
        "sk_orgao": list(zip(df.get("cod_gestora"), df["ano"])),
        "sk_modalidade": df.get("descricao_modalidade").map(modalidade_map) if "descricao_modalidade" in df else None,
        "sk_tempo": df["data_assinatura"].map(tempo_map),
        "valor_contrato": pd.to_numeric(df.get("valor_contrato"), errors="coerce"),
        "valor_pago": pd.to_numeric(df.get("calculated_valor_pago"), errors="coerce"),
        "valor_empenhado": pd.to_numeric(df.get("calculated_valor_empenhado"), errors="coerce"),
        "status": df.get("descricao_situacao"),
        "flag_emergency": df.get("emergency").fillna(False).astype(bool) if "emergency" in df else False,
    })
    fato["sk_orgao"] = fato["sk_orgao"].map(orgao_map)

    columns = list(fato.columns)
    update_cols = [c for c in columns if c not in ("id_contrato_origem", "ano")]
    _bulk_upsert(engine, "dw.fato_contrato", columns, fato, ["id_contrato_origem", "ano"], update_cols=update_cols)
    return len(fato)


def load_fato_empenho(engine, empenhos_enriched: pd.DataFrame, dim_orgao, dim_tempo):
    if empenhos_enriched.empty:
        return 0

    df = empenhos_enriched.copy()
    # Empenhos não tem PK real -- "id" sozinho se repete em anos diferentes
    # (mesma limitação documentada no README/dicionario-dados.md). Chave real
    # de negócio é (id, ano), igual ao que silver_transformer já usa pro dedup.
    df = df.drop_duplicates(subset=["id", "ano"], keep="last")
    orgao_map = dim_orgao.set_index(["codigo", "ano"])["sk_orgao"].to_dict()
    tempo_map = dim_tempo.assign(data=dim_tempo["data"].astype(str)).set_index("data")["sk_tempo"].to_dict()

    fato = pd.DataFrame({
        "id_empenho_origem": df["id"],
        "ano": df["ano"],
        "sk_orgao": list(zip(df.get("codigoug"), df.get("ano"))),
        "sk_tempo": df["dataemissao"].map(tempo_map),
        "cod_contrato": df.get("codprocesso"),
        "valor": pd.to_numeric(df.get("valor"), errors="coerce"),
        "modalidade": df.get("modalidade"),
    })
    fato["sk_orgao"] = fato["sk_orgao"].map(orgao_map)

    columns = list(fato.columns)
    update_cols = [c for c in columns if c not in ("id_empenho_origem", "ano")]
    _bulk_upsert(engine, "dw.fato_empenho", columns, fato, ["id_empenho_origem", "ano"], update_cols=update_cols)
    return len(fato)


def load_dw(read_source_fn=read_source):
    """Lê todo o histórico da Silver e carrega dimensões + fatos no DW (Gold).

    `read_source_fn(source) -> pd.DataFrame` é injetável: o default lê os Parquet
    da Silver (backend `local`/dev, via `silver_storage.read_source`); o job
    PySpark (`src/spark_jobs/gold_job.py`) injeta um reader que lê as tabelas
    Iceberg no HDFS via Spark e devolve pandas — mantendo esta orquestração
    (ordem dimensões -> fatos, idempotência) como fonte única de verdade.
    """
    engine = _get_engine()
    apply_ddl(engine)

    contratos = read_source_fn("contratos")
    unidade_gestora = read_source_fn("unidade_gestora")
    empenhos = read_source_fn("empenhos")

    dim_credor = load_dim_credor(engine, contratos)
    dim_orgao = load_dim_orgao(engine, unidade_gestora)
    dim_modalidade = load_dim_modalidade(engine, contratos)
    dim_tempo = load_dim_tempo(engine, [
        contratos.get("data_assinatura"),
        empenhos.get("dataemissao"),
    ])

    contratos_written = load_fato_contrato(engine, contratos, dim_credor, dim_orgao, dim_modalidade, dim_tempo)

    empenhos_enriched = enrich_with_unidade_gestora(empenhos, unidade_gestora) if not empenhos.empty else empenhos
    empenhos_written = load_fato_empenho(engine, empenhos_enriched, dim_orgao, dim_tempo)

    result = {
        "dim_credor": len(dim_credor),
        "dim_orgao": len(dim_orgao),
        "dim_modalidade": len(dim_modalidade),
        "dim_tempo": len(dim_tempo),
        "fato_contrato": contratos_written,
        "fato_empenho": empenhos_written,
    }
    logger.info("Carga do DW concluída: %s", result)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(load_dw())
