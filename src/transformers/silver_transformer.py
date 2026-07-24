"""
Transformação Bronze → Silver: normalização de datas, CNPJ/CPF e deduplicação.

Escopo (Fase 2, tarefas 13/14 — Carlos): implementação inicial de apoio à
DAG 2 (Jaime), adiantada em 19/07/2026 para destravar a orquestração antes do
Carlos assumir oficialmente as tarefas. Regras seguem a seção "Mapeamento
Bronze → Gold e regras de normalização" de `documentacao/dicionario-dados.md`
— marcadas lá como `[proposto — a validar]`, o Carlos deve revisar antes da
Fase 4.
"""

import logging

from src.extractors.storage import find_data_extracao_dirs, list_json_files, read_json_records
from src.transformers.rules import (
    API_DATE_FIELDS,
    DEDUP_KEYS,
    PARTITION_DATE_FIELD,
    POSTGRES_DATE_FIELDS,
    normalize_api_date,
    normalize_cnpj_cpf,
    normalize_postgres_date,
)
from src.transformers.silver_storage import write_parquet_records

logger = logging.getLogger(__name__)

# Constantes e normalizações vivem em src/transformers/rules.py — reexportadas aqui
# (API_DATE_FIELDS, POSTGRES_DATE_FIELDS, PARTITION_DATE_FIELD, DEDUP_KEYS) para
# compatibilidade e compartilhadas com o job PySpark (src/spark_jobs/silver_job.py).


def transform_record(source, record):
    """Aplica normalização de data e, para `contratos`, de CNPJ/CPF. Não muta o dict recebido."""
    record = dict(record)

    for field in API_DATE_FIELDS & record.keys():
        record[field] = normalize_api_date(record[field])
    for field in POSTGRES_DATE_FIELDS & record.keys():
        record[field] = normalize_postgres_date(record[field])

    if source == "contratos":
        record["cnpj_cpf_normalizado"], record["tipo_credor"] = normalize_cnpj_cpf(record)

    return record


def _dedup(source, records):
    """Remove duplicatas por chave lógica, mantendo a última ocorrência lida.

    Escopo: dedup *dentro* do lote lido nesta execução (uma `data_extracao`).
    Dedup entre execuções (mesmo `id`/`ano` reaparecendo em `data_extracao`s
    diferentes, mantendo a mais recente — regra documentada em
    dicionario-dados.md) fica pendente: exigiria ler o histórico já persistido
    na Silver, não só a partição do dia corrente.
    """
    key_fields = DEDUP_KEYS[source]
    deduped = {}
    for record in records:
        key = tuple(record.get(field) for field in key_fields)
        deduped[key] = record
    removed = len(records) - len(deduped)
    if removed:
        logger.info("'%s': %s registro(s) duplicado(s) removido(s) (chave %s)", source, removed, key_fields)
    return list(deduped.values())


def _partition_key(source, record):
    field = PARTITION_DATE_FIELD[source]
    value = record.get(field) if field else None
    if not value or len(value) < 7:
        return None, None
    return value[:4], value[5:7]


def transform_source(source, run_date):
    """Lê a Bronze de `source` para `run_date`, transforma e grava Parquet na Silver.

    Retorna contagens — seguro para XCom (nunca os registros em si).
    """
    if source not in DEDUP_KEYS:
        raise ValueError(f"Fonte '{source}' fora do escopo de transformação (esperado: {list(DEDUP_KEYS)})")

    partitions = find_data_extracao_dirs(source, run_date)
    files = [path for partition in partitions for path in list_json_files(partition)]

    records = [
        transform_record(source, record)
        for relative_path in files
        for record in read_json_records(relative_path)
    ]
    deduped = _dedup(source, records)

    groups = {}
    for record in deduped:
        groups.setdefault(_partition_key(source, record), []).append(record)

    silver_files = 0
    for (ano, mes), group_records in groups.items():
        if ano and mes:
            relative_path = f"{source}/ano={ano}/mes={mes}/data_extracao={run_date}/part_0001.parquet"
        else:
            relative_path = f"{source}/data_extracao={run_date}/part_0001.parquet"
        write_parquet_records(relative_path, group_records)
        silver_files += 1

    return {
        "source": source,
        "bronze_files": len(files),
        "records_read": len(records),
        "records_written": len(deduped),
        "silver_files": silver_files,
    }


def transform_bronze_to_silver(run_date):
    """Transforma todas as fontes da Bronze para a Silver, para uma `data_extracao`."""
    return {source: transform_source(source, run_date) for source in DEDUP_KEYS}
