"""Job PySpark: Bronze (JSON raw, HDFS) -> Silver (tabelas Iceberg, HMS/HDFS).

Substitui o caminho pandas (`src/transformers/silver_transformer.py`) no
pipeline HDFS. Reusa as MESMAS regras de normalização/dedup/particionamento de
`src/transformers/rules.py` (fonte única de verdade), agora como expressões de
coluna do Spark.

Ganho central vs. o caminho pandas: o dedup deixa de ser só "dentro de uma
execução" — o `MERGE INTO` na tabela Iceberg deduplica **entre execuções** pela
chave de negócio (`DEDUP_KEYS`), resolvendo a limitação documentada em
`silver_transformer._dedup` e o `drop_duplicates` defensivo do `dw_loader`.

Uso: spark-submit src/spark_jobs/silver_job.py --run-date YYYY-MM-DD
"""

import argparse
import logging
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.spark_jobs.spark_session import CATALOG, NAMESPACE, build_session, table_fqn  # noqa: E402
from src.transformers.rules import (  # noqa: E402
    API_DATE_FIELDS,
    DEDUP_KEYS,
    PARTITION_DATE_FIELD,
    POSTGRES_DATE_FIELDS,
)

logger = logging.getLogger(__name__)

BRONZE_BASE_PATH = os.environ.get("BRONZE_BASE_PATH", "/bronze")

# Colunas de particionamento Iceberg por fonte. empenhos/OB/contratos por
# (ano, mes); unidade_gestora (tabela de referência, sem coluna de data) só por
# ano — mesmo racional do particionamento pandas (rules.PARTITION_DATE_FIELD).
PARTITION_COLS = {
    "empenhos": ["ano", "mes"],
    "ordem_bancaria_orcamentaria": ["ano", "mes"],
    "contratos": ["ano", "mes"],
    "unidade_gestora": ["ano"],
}


def _run_date_dirs(spark, source: str, run_date: str) -> list:
    """Diretórios `data_extracao=<run_date>` de `source`, via glob no HDFS.

    Cobre o layout plano (`contratos`/`unidade_gestora`: `data_extracao=` direto)
    e o aninhado por `ano=/mes=` (`empenhos`/`ordem_bancaria_orcamentaria`). Ler
    só a(s) partição(ões) do dia — em vez de `recursiveFileLookup` na fonte
    inteira e filtrar depois — evita varrer todo o histórico (empenhos ~1,38M/7GB)
    a cada execução, o que estourava a memória.
    """
    base = f"{BRONZE_BASE_PATH.rstrip('/')}/{source}"
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(base), hconf)
    dirs = []
    for pattern in (f"{base}/data_extracao={run_date}", f"{base}/*/*/data_extracao={run_date}"):
        statuses = fs.globStatus(jvm.org.apache.hadoop.fs.Path(pattern))
        if statuses:
            dirs.extend(st.getPath().toString() for st in statuses)
    return dirs


def read_bronze(spark, source: str, run_date: str):
    """Lê os JSON da Bronze de `source` para `run_date`, ou None se não houver dados.

    Os arquivos Bronze são *arrays* JSON (`json.dumps(records)`) — daí
    `multiline=true`. Lê apenas os diretórios `data_extracao=<run_date>` (ver
    `_run_date_dirs`), não a fonte inteira.
    """
    dirs = _run_date_dirs(spark, source, run_date)
    if not dirs:
        logger.warning("Bronze sem partição data_extracao=%s para '%s' — pulando", run_date, source)
        return None

    df = spark.read.option("multiline", "true").json(dirs)
    if not df.take(1):
        logger.warning("Nenhum registro em '%s' para data_extracao=%s", source, run_date)
        return None
    return df


def _non_empty(col):
    """Devolve a coluna quando não-nula e não-vazia, senão NULL — equivale ao `or` do pandas."""
    return F.when(col.isNotNull() & (col.cast("string") != ""), col.cast("string"))


def normalize(df, source: str):
    """Normaliza datas (e CNPJ/CPF em contratos), reusando as regras de `rules.py`."""
    # Normaliza datas da API para ISO 'YYYY-MM-DD':
    #  1. DD/MM/YYYY -> YYYY-MM-DD (regexp ancorado; só altera o que casa exato);
    #  2. valores já ISO com hora/timezone (ex: '2026-07-21T00:00:00.000-03:00')
    #     são cortados para os 10 primeiros chars -> '2026-07-21'.
    # O que não casar nenhum dos dois é mantido como veio (não quebra a carga).
    for field in API_DATE_FIELDS:
        if field in df.columns:
            converted = F.regexp_replace(F.col(field), r"^(\d{2})/(\d{2})/(\d{4})$", r"$3-$2-$1")
            df = df.withColumn(
                field,
                F.when(converted.rlike(r"^\d{4}-\d{2}-\d{2}"), F.substring(converted, 1, 10)).otherwise(converted),
            )
    # TEXT 'YYYY-MM-DD HH:MM:SS.mmm' -> corte dos 10 primeiros chars (já ISO).
    for field in POSTGRES_DATE_FIELDS:
        if field in df.columns:
            df = df.withColumn(field, F.substring(F.col(field), 1, 10))

    if source == "contratos":
        plain = _non_empty(F.col("plain_cpf_cnpj_financiador")) if "plain_cpf_cnpj_financiador" in df.columns else F.lit(None)
        masked = _non_empty(F.col("cpf_cnpj_financiador")) if "cpf_cnpj_financiador" in df.columns else F.lit(None)
        digits = F.regexp_replace(F.coalesce(plain, masked, F.lit("")), r"[^0-9]", "")
        df = df.withColumn("cnpj_cpf_normalizado", digits)
        df = df.withColumn(
            "tipo_credor",
            F.when(F.length("cnpj_cpf_normalizado") == 11, F.lit("PF"))
            .when(F.length("cnpj_cpf_normalizado") == 14, F.lit("PJ"))
            .otherwise(F.lit("INVALIDO")),
        )
    return df


def add_partitions(df, source: str):
    """Adiciona as colunas de partição (ano [, mes]) conforme PARTITION_COLS."""
    date_field = PARTITION_DATE_FIELD[source]
    if source == "contratos":
        # contratos não tem `ano` na origem — deriva de data_assinatura (já ISO).
        df = df.withColumn("ano", F.substring(F.col(date_field), 1, 4).cast(IntegerType()))
    elif "ano" in df.columns:
        # empenhos/OB/unidade_gestora têm `ano` real na origem (também chave de dedup).
        df = df.withColumn("ano", F.col("ano").cast(IntegerType()))
    else:
        df = df.withColumn("ano", F.lit(None).cast(IntegerType()))

    if "mes" in PARTITION_COLS[source]:
        df = df.withColumn("mes", F.substring(F.col(date_field), 6, 2).cast(IntegerType()))
    return df


def dedup_batch(df, source: str):
    """Dedup do lote pela chave de negócio — obrigatório antes do MERGE (fonte com chave única)."""
    return df.dropDuplicates(list(DEDUP_KEYS[source]))


def write_source(spark, source: str, df) -> None:
    """Cria a tabela Iceberg (1ª carga) ou faz MERGE upsert (cargas seguintes)."""
    fqn = table_fqn(source)
    part_cols = PARTITION_COLS[source]
    keys = list(DEDUP_KEYS[source])

    if not spark.catalog.tableExists(fqn):
        (
            df.writeTo(fqn)
            .using("iceberg")
            .partitionedBy(*[F.col(c) for c in part_cols])
            .create()
        )
        logger.info("Tabela Iceberg criada: %s", fqn)
        return

    # Evolução de schema: adiciona ao alvo colunas novas que apareçam no lote.
    target_fields = {f.name: f.dataType for f in spark.table(fqn).schema.fields}
    for field in df.schema.fields:
        if field.name not in target_fields:
            spark.sql(f"ALTER TABLE {fqn} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
            target_fields[field.name] = field.dataType

    # Alinha o lote ao schema completo do alvo (colunas ausentes -> NULL tipado)
    # para o UPDATE SET * / INSERT * do MERGE casarem coluna a coluna.
    aligned = df
    for name, dtype in target_fields.items():
        if name not in aligned.columns:
            aligned = aligned.withColumn(name, F.lit(None).cast(dtype))
    aligned = aligned.select(*target_fields.keys())
    aligned.createOrReplaceTempView("_silver_batch")

    on_clause = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    spark.sql(
        f"""
        MERGE INTO {fqn} t
        USING _silver_batch s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    logger.info("MERGE em %s concluído (chave %s)", fqn, keys)


def run(run_date: str) -> dict:
    spark = build_session(f"silver_transform_{run_date}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

    summary = {}
    try:
        for source in DEDUP_KEYS:
            df = read_bronze(spark, source, run_date)
            if df is None:
                summary[source] = 0
                continue
            df = dedup_batch(add_partitions(normalize(df, source), source), source)
            summary[source] = df.count()
            write_source(spark, source, df)
    finally:
        spark.stop()

    logger.info("Silver concluída para run_date=%s: %s", run_date, summary)
    return summary


def _parse_args():
    parser = argparse.ArgumentParser(description="Transformação Bronze -> Silver (Iceberg)")
    parser.add_argument("--run-date", dest="run_date", required=True, help="data_extracao a processar (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(_parse_args().run_date)
