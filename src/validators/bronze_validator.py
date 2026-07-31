"""
Validação da camada Bronze — schema e completude mínima dos arquivos ingeridos.

Confere, para uma `data_extracao`, que cada fonte tem ao menos um arquivo
gravado, que as colunas obrigatórias estão presentes em todos os registros e
que a contagem mínima esperada foi atingida.
"""

import logging
import os
import random
from typing import Any

from src.extractors.storage import find_data_extracao_dirs, list_json_files, read_json_records

logger = logging.getLogger(__name__)

# Tamanho da amostra de registros checados por arquivo, por fonte. `0`
# (padrão) checa 100%. Setar via `BRONZE_VALIDATE_SAMPLE_SIZE` reduz o custo
# de CPU numa task single-thread do Airflow sem afetar a contagem total
# retornada (que não depende de checar cada registro, só de `len(records)`).
RECORD_SAMPLE_SIZE = int(os.environ.get("BRONZE_VALIDATE_SAMPLE_SIZE", "0"))

# Colunas mínimas exigidas por fonte para considerar o schema íntegro.
REQUIRED_COLUMNS = {
    "empenhos": {"id", "ano", "dataemissao"},
    "ordem_bancaria_orcamentaria": {"id", "ano", "dataemissao"},
    "unidade_gestora": {"codigo", "ano"},
    # num_contrato NÃO entra aqui: contratos por dispensa/inexigibilidade sem
    # instrumento formal legitimamente vêm com num_contrato=null na API real.
    # A identificação do registro é `id`; num_contrato é só referência.
    "contratos": {"id", "valor_contrato", "data_assinatura", "cod_gestora"},
}

# Mínimo de registros esperado quando o caller não passa
# `min_records_by_source`. Só faz sentido para `unidade_gestora`, a única
# fonte recarregada por inteiro a cada execução — as demais são
# incrementais por watermark, com volume diário legitimamente variável
# (inclusive zero), então um mínimo fixo geraria falso positivo.
DEFAULT_MIN_RECORDS_BY_SOURCE = {
    "unidade_gestora": 4000,
}


class BronzeValidationError(RuntimeError):
    """Falha de validação de schema ou completude na camada Bronze."""


def _records_to_check(records: list[dict[str, Any]], sample_size: int) -> list[dict[str, Any]]:
    """Amostra de `records` para a checagem de schema/completude.

    `sample_size` de 0 (ou >= len(records)) preserva o comportamento original —
    checa todos. Caso contrário, pega o início e o fim do lote (erro sistemático
    de um chunk tende a aparecer logo nos primeiros/últimos registros escritos)
    mais uma amostra aleatória do meio — determinística o bastante pra pegar
    problema recorrente, sem custar O(todos os registros).
    """
    if not sample_size or len(records) <= sample_size:
        return records

    edge = sample_size // 3
    head = records[:edge]
    tail = records[-edge:] if edge else []
    middle_pool = records[edge : len(records) - edge]
    middle_size = max(sample_size - len(head) - len(tail), 0)
    middle = random.sample(middle_pool, min(middle_size, len(middle_pool)))
    return head + middle + tail


def validate_source(
    source: str,
    run_date: str,
    required_columns: set[str] | None = None,
    min_records: int = 0,
    sample_size: int | None = None,
) -> dict[str, Any]:
    """Valida os arquivos de uma fonte para uma `data_extracao` específica.

    Busca recursivamente por `data_extracao={run_date}` sob a raiz da fonte —
    cobre tanto o layout plano (`contratos/`, `unidade_gestora/`) quanto o
    particionamento por `ano=/mes=` (`empenhos/`, `ordem_bancaria_orcamentaria/`),
    onde uma mesma `data_extracao` pode se espalhar por várias partições.

    `unidade_gestora` é referência completa e pode legitimamente vir vazia em
    bases de teste, mas as demais fontes precisam ter ao menos um arquivo —
    ausência total de arquivo indica que a extração não rodou.

    `sample_size` (default: `RECORD_SAMPLE_SIZE`, controlado por
    `BRONZE_VALIDATE_SAMPLE_SIZE`) limita quantos registros de CADA arquivo têm
    o schema checado — `0` checa todos (comportamento original). A contagem
    total retornada em `records` sempre reflete o volume real do arquivo,
    independente da amostra usada na checagem.
    """
    if source not in REQUIRED_COLUMNS:
        raise ValueError(f"Fonte '{source}' fora do escopo de validação (esperado: {list(REQUIRED_COLUMNS)})")
    required_columns = required_columns or REQUIRED_COLUMNS[source]
    sample_size = RECORD_SAMPLE_SIZE if sample_size is None else sample_size

    partitions = find_data_extracao_dirs(source, run_date)
    files = [path for partition in partitions for path in list_json_files(partition)]
    if not files:
        raise BronzeValidationError(
            f"'{source}': nenhum arquivo encontrado para data_extracao={run_date} " f"(busca recursiva sob '{source}/')"
        )

    total_records = 0
    for relative_path in files:
        records = read_json_records(relative_path)
        for record in _records_to_check(records, sample_size):
            missing = required_columns - record.keys()
            if missing:
                raise BronzeValidationError(
                    f"'{source}' ({relative_path}): colunas obrigatórias ausentes: {sorted(missing)}"
                )
            # Coluna presente mas vazia (None ou string em branco) é completude
            # tão quebrada quanto coluna ausente — ex: dataemissao="" travaria o
            # particionamento ano=/mes= silenciosamente lá na frente.
            blank = {
                column
                for column in required_columns
                if record[column] is None or (isinstance(record[column], str) and not record[column].strip())
            }
            if blank:
                raise BronzeValidationError(
                    f"'{source}' ({relative_path}): colunas obrigatórias vazias: {sorted(blank)}"
                )
        total_records += len(records)

    if total_records < min_records:
        raise BronzeValidationError(
            f"'{source}': {total_records} registros para data_extracao={run_date}, esperado >= {min_records}"
        )

    logger.info(
        "Bronze validada [%s]: %s partição(ões), %s arquivo(s), %s registro(s)%s",
        source,
        len(partitions),
        len(files),
        total_records,
        f" (schema checado por amostra de até {sample_size}/arquivo)" if sample_size else "",
    )
    return {"source": source, "partitions": len(partitions), "files": len(files), "records": total_records}


def validate_bronze(run_date: str, min_records_by_source: dict[str, int] | None = None) -> dict[str, Any]:
    """Valida todas as fontes da Bronze (empenhos, OB, unidade_gestora, contratos) para `run_date`.

    Sem `min_records_by_source` explícito, usa `DEFAULT_MIN_RECORDS_BY_SOURCE`
    (hoje só `unidade_gestora`, a única fonte recarregada por inteiro a cada
    execução — ver comentário na constante). Passe um dict para sobrescrever
    fonte a fonte; fontes omitidas caem no default de `DEFAULT_MIN_RECORDS_BY_SOURCE`.

    Levanta `BronzeValidationError` na primeira fonte inválida. Retorna um
    resumo por fonte — seguro para XCom (apenas contagens, nunca registros).
    """
    min_records_by_source = {**DEFAULT_MIN_RECORDS_BY_SOURCE, **(min_records_by_source or {})}
    return {
        source: validate_source(source, run_date, min_records=min_records_by_source.get(source, 0))
        for source in REQUIRED_COLUMNS
    }
