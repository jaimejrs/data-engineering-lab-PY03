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


def find_data_extracao_dirs(relative_root: str, run_date: str) -> list:
    """Encontra, recursivamente, todos os diretórios `data_extracao={run_date}`
    sob `relative_root` da Bronze.

    Cobre tanto fontes com particionamento plano (`contratos/`,
    `unidade_gestora/` — `data_extracao=` direto sob a raiz da fonte) quanto
    por `ano=/mes=` (`empenhos/`, `ordem_bancaria_orcamentaria/`, onde uma
    mesma `data_extracao` pode se espalhar por vários `ano=/mes=`), sem
    precisar saber de antemão qual layout cada fonte usa. Retorna caminhos
    relativos a `BRONZE_BASE_PATH`, prontos para passar a `list_json_files`.
    """
    target_name = f"data_extracao={run_date}"
    full_root = f"{BRONZE_BASE_PATH.rstrip('/')}/{relative_root.strip('/')}"
    # Normaliza para "/" antes de montar o prefixo: no backend local, em
    # Windows, BRONZE_BASE_PATH vem com "\" (os.walk também retorna "\"), mas
    # os caminhos HDFS e o restante do módulo usam "/" — sem isso o strip do
    # prefixo silenciosamente não bate e o caminho relativo vira o absoluto.
    base_prefix = BRONZE_BASE_PATH.replace("\\", "/").rstrip("/") + "/"

    if BRONZE_BACKEND == "hdfs":
        client = _get_hdfs_client()
        try:
            dirpaths = [dirpath for dirpath, _dirs, _files in client.walk(full_root)]
        except Exception:
            return []
    else:
        if not os.path.isdir(full_root):
            return []
        dirpaths = [
            dirpath.replace(os.sep, "/") for dirpath, _dirs, _files in os.walk(full_root)
        ]

    matches = [
        dirpath[len(base_prefix):] if dirpath.startswith(base_prefix) else dirpath.lstrip("/")
        for dirpath in dirpaths
        if dirpath.rstrip("/").rsplit("/", 1)[-1] == target_name
    ]
    return sorted(matches)


def read_json_records(relative_path: str) -> list:
    """Lê de volta uma lista de registros gravada na Bronze por `write_json_records`."""
    full_path = f"{BRONZE_BASE_PATH.rstrip('/')}/{relative_path.lstrip('/')}"

    if BRONZE_BACKEND == "hdfs":
        client = _get_hdfs_client()
        with client.read(full_path, encoding="utf-8") as reader:
            return json.load(reader)

    with open(full_path, "r", encoding="utf-8") as fh:
        return json.load(fh)
