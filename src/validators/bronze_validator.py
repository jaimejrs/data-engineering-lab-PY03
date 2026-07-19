"""
Validação da camada Bronze — schema e completude mínima dos arquivos ingeridos.

Escopo (Fase 1, task validate_bronze): confere, para uma `data_extracao`, que
cada fonte tem ao menos um arquivo gravado, que as colunas obrigatórias estão
presentes em todos os registros e que a contagem mínima esperada foi atingida.

Implementação inicial de apoio à DAG 1 (Membro 1) — a cargo do Membro 4
(Benjamim) refinar regras de completude/qualidade adicionais.
"""

import logging

from src.extractors.storage import find_data_extracao_dirs, list_json_files, read_json_records

logger = logging.getLogger(__name__)

# Colunas mínimas exigidas por fonte para considerar o schema íntegro.
REQUIRED_COLUMNS = {
    "empenhos": {"id", "ano", "dataemissao"},
    "ordem_bancaria_orcamentaria": {"id", "ano", "dataemissao"},
    "unidade_gestora": {"codigo", "ano"},
    "contratos": {"id", "num_contrato", "valor_contrato", "data_assinatura", "cod_gestora"},
}


class BronzeValidationError(RuntimeError):
    """Falha de validação de schema ou completude na camada Bronze."""


def validate_source(source, run_date, required_columns=None, min_records=0):
    """Valida os arquivos de uma fonte para uma `data_extracao` específica.

    Busca recursivamente por `data_extracao={run_date}` sob a raiz da fonte —
    cobre tanto o layout plano (`contratos/`, `unidade_gestora/`) quanto o
    particionamento por `ano=/mes=` (`empenhos/`, `ordem_bancaria_orcamentaria/`),
    onde uma mesma `data_extracao` pode se espalhar por várias partições.

    `unidade_gestora` é referência completa e pode legitimamente vir vazia em
    bases de teste, mas as demais fontes precisam ter ao menos um arquivo —
    ausência total de arquivo indica que a extração não rodou.
    """
    if source not in REQUIRED_COLUMNS:
        raise ValueError(f"Fonte '{source}' fora do escopo de validação (esperado: {list(REQUIRED_COLUMNS)})")
    required_columns = required_columns or REQUIRED_COLUMNS[source]

    partitions = find_data_extracao_dirs(source, run_date)
    files = [path for partition in partitions for path in list_json_files(partition)]
    if not files:
        raise BronzeValidationError(
            f"'{source}': nenhum arquivo encontrado para data_extracao={run_date} "
            f"(busca recursiva sob '{source}/')"
        )

    total_records = 0
    for relative_path in files:
        records = read_json_records(relative_path)
        for record in records:
            missing = required_columns - record.keys()
            if missing:
                raise BronzeValidationError(
                    f"'{source}' ({relative_path}): colunas obrigatórias ausentes: {sorted(missing)}"
                )
        total_records += len(records)

    if total_records < min_records:
        raise BronzeValidationError(
            f"'{source}': {total_records} registros para data_extracao={run_date}, esperado >= {min_records}"
        )

    logger.info(
        "Bronze validada [%s]: %s partição(ões), %s arquivo(s), %s registro(s)",
        source, len(partitions), len(files), total_records,
    )
    return {"source": source, "partitions": len(partitions), "files": len(files), "records": total_records}


def validate_bronze(run_date, min_records_by_source=None):
    """Valida todas as fontes da Bronze (empenhos, OB, unidade_gestora, contratos) para `run_date`.

    Levanta `BronzeValidationError` na primeira fonte inválida. Retorna um
    resumo por fonte — seguro para XCom (apenas contagens, nunca registros).
    """
    min_records_by_source = min_records_by_source or {}
    return {
        source: validate_source(source, run_date, min_records=min_records_by_source.get(source, 0))
        for source in REQUIRED_COLUMNS
    }
