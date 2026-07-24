"""Reconciliação Bronze -> Silver (item 6 de docs/06-analise-critica.md).

Bronze é JSON em arquivo — contar exato exige reler tudo (o mesmo custo que
`validate_bronze` já paga, ver item 7). Em vez de reler a Bronze de novo só
para reconciliar, `record_bronze_counts` persiste a contagem que
`validate_bronze` já calculou como efeito colatear da validação normal — uma
linha por `(fonte, data_extracao)`, custo extra é um `DELETE`+`INSERT`
pequeno, não uma releitura.

A comparação com a Silver (que é Iceberg, barata de contar via Trino) fica no
dbt: `dbt/models/marts/bronze_silver_reconciliacao.sql` soma as contagens de
Bronze aqui persistidas e compara com a contagem ao vivo da Silver.
"""

import logging

from src import trino_io

logger = logging.getLogger(__name__)

TABLE = "iceberg.gold.bronze_ingestao"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        fonte varchar,
        data_extracao varchar,
        registros bigint,
        gravado_em timestamp
    )
"""


def record_bronze_counts(run_date: str, counts_by_source: dict) -> None:
    """Grava/atualiza a contagem de Bronze por fonte para `run_date`.

    `counts_by_source`: `{fonte: registros}` — ex: o resultado de
    `validate_bronze()`, já reduzido a `{source: info["records"]}`. Upsert por
    `(fonte, data_extracao)`: reprocessar o mesmo `data_extracao` (retry ou
    backfill manual) atualiza a contagem em vez de duplicar a linha e inflar a
    soma usada na reconciliação.
    """
    trino_io.execute(DDL)
    for fonte, registros in counts_by_source.items():
        trino_io.execute(f"DELETE FROM {TABLE} WHERE fonte = '{fonte}' AND data_extracao = '{run_date}'")
        trino_io.execute(
            f"INSERT INTO {TABLE} (fonte, data_extracao, registros, gravado_em) "
            f"VALUES ('{fonte}', '{run_date}', {int(registros)}, CURRENT_TIMESTAMP)"
        )
    logger.info("Reconciliação: contagens de Bronze gravadas para data_extracao=%s: %s", run_date, counts_by_source)
