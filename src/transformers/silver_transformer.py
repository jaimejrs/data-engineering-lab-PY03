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
import re
from datetime import datetime

from src.extractors.storage import find_data_extracao_dirs, list_json_files, read_json_records
from src.transformers.silver_storage import write_parquet_records

logger = logging.getLogger(__name__)

# Campos de data por formato de origem (ver dicionario-dados.md).
API_DATE_FIELDS = {"data_assinatura", "data_inicio", "data_termino", "data_publicacao_portal", "data_rescisao"}
POSTGRES_DATE_FIELDS = {"dataemissao"}

# Campo usado para reparticionar a Silver por ano=/mes=, por fonte (None = sem partição).
PARTITION_DATE_FIELD = {
    "empenhos": "dataemissao",
    "ordem_bancaria_orcamentaria": "dataemissao",
    "contratos": "data_assinatura",
    "unidade_gestora": None,
}

# Chave lógica de deduplicação por fonte — nenhuma tem PK real na origem (ver README.md).
DEDUP_KEYS = {
    "empenhos": ("id", "ano"),
    "ordem_bancaria_orcamentaria": ("id", "ano"),
    "contratos": ("id",),
    "unidade_gestora": ("codigo", "ano"),
}


def _normalize_api_date(value):
    """'DD/MM/YYYY' -> 'YYYY-MM-DD'. Preserva None/vazio (ex: data_rescisao de contrato ativo)."""
    if not value:
        return value
    try:
        return datetime.strptime(value, "%d/%m/%Y").date().isoformat()
    except ValueError:
        logger.warning("Data fora do formato DD/MM/YYYY esperado: %r — mantendo valor original", value)
        return value


def _normalize_postgres_date(value):
    """TEXT 'YYYY-MM-DD HH:MM:SS.mmm' -> 'YYYY-MM-DD' (corte de prefixo, já é ISO 8601)."""
    return value[:10] if value else value


def _normalize_cnpj_cpf(record):
    """Só dígitos, priorizando o campo já sem máscara da API.

    tipo: "PF" (11 dígitos), "PJ" (14 dígitos) ou "INVALIDO" (outro tamanho —
    não descarta a linha, só sinaliza, ver regra em dicionario-dados.md).
    """
    raw = record.get("plain_cpf_cnpj_financiador") or record.get("cpf_cnpj_financiador") or ""
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 11:
        tipo = "PF"
    elif len(digits) == 14:
        tipo = "PJ"
    else:
        tipo = "INVALIDO"
    return digits, tipo


def transform_record(source, record):
    """Aplica normalização de data e, para `contratos`, de CNPJ/CPF. Não muta o dict recebido."""
    record = dict(record)

    for field in API_DATE_FIELDS & record.keys():
        record[field] = _normalize_api_date(record[field])
    for field in POSTGRES_DATE_FIELDS & record.keys():
        record[field] = _normalize_postgres_date(record[field])

    if source == "contratos":
        record["cnpj_cpf_normalizado"], record["tipo_credor"] = _normalize_cnpj_cpf(record)

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
