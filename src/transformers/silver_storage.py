"""Camada de escrita para a Silver — Parquet, local (dev) ou HDFS via WebHDFS.

Mesmo padrão de `src/extractors/storage.py` (Bronze): backend configurável por
variável de ambiente, sem acoplar o restante do código a local vs. HDFS.
"""

import io
import logging
import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SILVER_BACKEND = os.environ.get("SILVER_STORAGE_BACKEND", "local")  # "local" ou "hdfs"
SILVER_BASE_PATH = os.environ.get("SILVER_BASE_PATH", "./data/silver")
HDFS_WEBHDFS_URL = os.environ.get("HDFS_WEBHDFS_URL")
HDFS_USER = os.environ.get("HDFS_USER", "hdfs")


def _get_hdfs_client():
    from hdfs import InsecureClient  # import tardio: só exige a lib quando o backend é hdfs

    if not HDFS_WEBHDFS_URL:
        raise RuntimeError("HDFS_WEBHDFS_URL não configurada mas SILVER_STORAGE_BACKEND=hdfs")
    return InsecureClient(HDFS_WEBHDFS_URL, user=HDFS_USER)


def write_parquet_records(relative_path: str, records: list) -> str:
    """Grava uma lista de registros como Parquet na camada Silver. Retorna o caminho completo gravado."""
    df = pd.DataFrame.from_records(records)
    full_path = f"{SILVER_BASE_PATH.rstrip('/')}/{relative_path.lstrip('/')}"

    if SILVER_BACKEND == "hdfs":
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        client = _get_hdfs_client()
        client.write(full_path, data=buffer.getvalue(), overwrite=True)
    else:
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        df.to_parquet(full_path, index=False)

    logger.info("Silver [%s]: %s (%s registros)", SILVER_BACKEND, full_path, len(records))
    return full_path
