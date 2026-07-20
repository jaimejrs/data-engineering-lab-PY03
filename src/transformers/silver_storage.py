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


def find_parquet_files(relative_root: str) -> list:
    """Lista recursivamente todos os `.parquet` sob `relative_root`, em qualquer partição.

    Usado pela carga do DW (Gold): ao contrário da Bronze/Silver por dia
    (`data_extracao`), o DW é cumulativo — precisa ler todo o histórico já
    persistido na Silver, não só uma execução.
    """
    full_root = f"{SILVER_BASE_PATH.rstrip('/')}/{relative_root.strip('/')}"
    base_prefix = SILVER_BASE_PATH.replace("\\", "/").rstrip("/") + "/"

    if SILVER_BACKEND == "hdfs":
        client = _get_hdfs_client()
        try:
            paths = [
                f"{dirpath}/{name}"
                for dirpath, _dirs, files in client.walk(full_root)
                for name in files
                if name.endswith(".parquet")
            ]
        except Exception:
            return []
    else:
        if not os.path.isdir(full_root):
            return []
        paths = [
            os.path.join(dirpath, name).replace(os.sep, "/")
            for dirpath, _dirs, files in os.walk(full_root)
            for name in files
            if name.endswith(".parquet")
        ]

    return sorted(
        path[len(base_prefix):] if path.startswith(base_prefix) else path.lstrip("/")
        for path in paths
    )


def read_parquet_df(relative_path: str) -> pd.DataFrame:
    """Lê de volta um Parquet gravado na Silver por `write_parquet_records`."""
    full_path = f"{SILVER_BASE_PATH.rstrip('/')}/{relative_path.lstrip('/')}"

    if SILVER_BACKEND == "hdfs":
        client = _get_hdfs_client()
        with client.read(full_path) as reader:
            return pd.read_parquet(io.BytesIO(reader.read()))

    return pd.read_parquet(full_path)


def read_source(source: str) -> pd.DataFrame:
    """Concatena todo o histórico Parquet já persistido de uma fonte da Silver."""
    files = find_parquet_files(source)
    if not files:
        return pd.DataFrame()
    return pd.concat([read_parquet_df(path) for path in files], ignore_index=True)
