"""Job PySpark: carga Gold (DW dimensional no Postgres) a partir da Silver Iceberg.

Lê as tabelas Iceberg `lakehouse.silver.*` via Spark, converte para pandas
(`toPandas`) e injeta em `dw_loader.load_dw(read_source_fn=...)` — reaproveitando
INTACTA toda a lógica de dimensões/fatos já testada de
`src/loaders/dw_loader.py` (que escreve no Postgres DW via psycopg2). Sem XCom
de DataFrame: o job faz tudo no driver, na ordem dimensões -> fatos.

Requisitos no driver (imagem do Spark): pandas, sqlalchemy, psycopg2, e a env
`DW_POSTGRES_URL` apontando para o Postgres DW; `sql/ddl_dw.sql` acessível no
caminho montado.

Uso: spark-submit src/spark_jobs/gold_job.py
"""

import logging
import os
import sys

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.loaders import dw_loader  # noqa: E402
from src.spark_jobs.spark_session import build_session, table_fqn  # noqa: E402

logger = logging.getLogger(__name__)


def _iceberg_reader(spark):
    """Reader injetável para `dw_loader.load_dw`: lê uma fonte da Silver Iceberg como pandas."""

    def read_source(source: str) -> pd.DataFrame:
        fqn = table_fqn(source)
        if not spark.catalog.tableExists(fqn):
            logger.warning("Tabela Iceberg ausente: %s — tratando como vazia", fqn)
            return pd.DataFrame()
        return spark.table(fqn).toPandas()

    return read_source


def run() -> dict:
    spark = build_session("gold_load")
    try:
        result = dw_loader.load_dw(read_source_fn=_iceberg_reader(spark))
    finally:
        spark.stop()
    logger.info("Carga Gold concluída: %s", result)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
