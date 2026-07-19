"""Camada de escrita para a Bronze — abstrai destino local (dev) ou HDFS via WebHDFS."""

import json
import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BRONZE_BACKEND = os.environ.get("BRONZE_STORAGE_BACKEND", "local")  # "local" ou "hdfs"
BRONZE_BASE_PATH = os.environ.get("BRONZE_BASE_PATH", "./data/bronze")
HDFS_WEBHDFS_URL = os.environ.get("HDFS_WEBHDFS_URL")  # ex: http://namenode:9870
HDFS_USER = os.environ.get("HDFS_USER", "hdfs")


def _get_hdfs_client():
    from hdfs import InsecureClient  # import tardio: só exige a lib quando o backend é hdfs

    if not HDFS_WEBHDFS_URL:
        raise RuntimeError("HDFS_WEBHDFS_URL não configurada mas BRONZE_STORAGE_BACKEND=hdfs")
    return InsecureClient(HDFS_WEBHDFS_URL, user=HDFS_USER)


def write_json_records(relative_path: str, records: list) -> str:
    """Grava uma lista de registros como JSON na camada Bronze. Retorna o caminho completo gravado."""
    payload = json.dumps(records, ensure_ascii=False, default=str)
    full_path = f"{BRONZE_BASE_PATH.rstrip('/')}/{relative_path.lstrip('/')}"

    if BRONZE_BACKEND == "hdfs":
        client = _get_hdfs_client()
        client.write(full_path, data=payload.encode("utf-8"), overwrite=True)
    else:
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as fh:
            fh.write(payload)

    logger.info("Bronze [%s]: %s (%s registros)", BRONZE_BACKEND, full_path, len(records))
    return full_path


def list_json_files(relative_dir: str) -> list:
    """Lista os arquivos `.json` diretamente sob um diretório da Bronze (não recursivo).

    Retorna caminhos relativos a `BRONZE_BASE_PATH`. Retorna lista vazia se o
    diretório não existir (execução sem dados no período, ou ainda não extraída).
    """
    full_dir = f"{BRONZE_BASE_PATH.rstrip('/')}/{relative_dir.strip('/')}"

    if BRONZE_BACKEND == "hdfs":
        client = _get_hdfs_client()
        try:
            names = client.list(full_dir)
        except Exception:
            return []
        return sorted(f"{relative_dir.strip('/')}/{name}" for name in names if name.endswith(".json"))

    if not os.path.isdir(full_dir):
        return []
    return sorted(
        f"{relative_dir.strip('/')}/{name}"
        for name in os.listdir(full_dir)
        if name.endswith(".json")
    )


def read_json_records(relative_path: str) -> list:
    """Lê de volta uma lista de registros gravada na Bronze por `write_json_records`."""
    full_path = f"{BRONZE_BASE_PATH.rstrip('/')}/{relative_path.lstrip('/')}"

    if BRONZE_BACKEND == "hdfs":
        client = _get_hdfs_client()
        with client.read(full_path, encoding="utf-8") as reader:
            return json.load(reader)

    with open(full_path, "r", encoding="utf-8") as fh:
        return json.load(fh)
