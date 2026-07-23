"""Regras de normalização/dedup Bronze -> Silver, compartilhadas.

Módulo *sem* dependências pesadas (só `re`/`datetime` da stdlib): pode ser
importado tanto pela transformação pandas (`silver_transformer.py`, backend
`local`/dev e testes) quanto pelo job PySpark (`src/spark_jobs/silver_job.py`,
pipeline HDFS/Iceberg). As constantes são a fonte única de verdade sobre quais
campos são data, como cada fonte é particionada e qual a chave lógica de dedup.
"""

import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# Campos de data por formato de origem (ver documentacao/dicionario-dados.md).
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


def normalize_api_date(value):
    """Normaliza data da API para ISO 'YYYY-MM-DD'. Preserva None/vazio.

    Cobre dois formatos vistos na fonte: 'DD/MM/YYYY' e ISO com hora/timezone
    (ex: '2026-07-21T00:00:00.000-03:00' -> '2026-07-21'). O que não casar
    nenhum é mantido como veio (com aviso), para não descartar o registro.
    """
    if not value:
        return value
    try:
        return datetime.strptime(value, "%d/%m/%Y").date().isoformat()
    except (ValueError, TypeError):
        pass
    if re.match(r"^\d{4}-\d{2}-\d{2}", str(value)):
        return str(value)[:10]
    logger.warning("Data fora dos formatos esperados (DD/MM/YYYY ou ISO): %r — mantendo original", value)
    return value


def normalize_postgres_date(value):
    """TEXT 'YYYY-MM-DD HH:MM:SS.mmm' -> 'YYYY-MM-DD' (corte de prefixo, já é ISO 8601)."""
    return value[:10] if value else value


def normalize_cnpj_cpf(record):
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
