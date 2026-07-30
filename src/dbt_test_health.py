"""Rastreamento histórico dos testes dbt de severidade WARN — achado da
auditoria de rigor científico de ML/DS de 30/07/2026: `dbt build` nunca falha
por causa de um teste WARN (decisão correta — ver os comentários em
`dbt/tests/assert_*.sql`, casos que não dá pra confirmar/corrigir
automaticamente contra a fonte original), mas isso também significa que a
contagem só aparece no log efêmero do build. Se o número de casos crescer
sistematicamente — sinal de um problema real piorando —, ninguém percebe sem
reler logs manualmente. Este módulo reimplementa as duas condições WARN
existentes e grava contagem + timestamp em `iceberg.audit`, pra dar pra
acompanhar tendência (ex: num dashboard Superset).

Reimplementa, não lê, os testes — `dbt build` roda num container efêmero
(`DockerOperator(auto_remove="success")`, ver `dags/dag_gold_load.py`), sem
volume montado pra `target/run_results.json` sobreviver ao fim da task; ler
os artefatos do dbt exigiria mudar esse mount, uma alteração de infra maior.
Se o WHERE de algum desses testes mudar em `dbt/tests/*.sql`, atualizar as
queries abaixo também (comentado em cada uma apontando pro arquivo espelhado).

Uso: python -m src.dbt_test_health [--no-persist]
"""

import argparse
import logging
from datetime import datetime, timezone

import pandas as pd

from src import trino_io

logger = logging.getLogger(__name__)

HEALTH_TABLE = "iceberg.audit.dbt_test_warnings_historico"

# Espelha dbt/tests/assert_empenho_negativo_monitorado.sql
QUERY_EMPENHO_NEGATIVO = """
SELECT COUNT(*) AS n
FROM iceberg.gold.fato_contrato
WHERE valor_empenhado < 0
"""

# Espelha dbt/tests/assert_pagamento_dentro_do_contratado.sql
QUERY_PAGAMENTO_ACIMA_CONTRATADO = """
SELECT COUNT(*) AS n
FROM iceberg.gold.fato_contrato
WHERE valor_contrato > 0
    AND (valor_pago > valor_contrato * 2 OR valor_empenhado > valor_contrato * 2)
    AND ABS(
        (valor_contrato + COALESCE(valor_aditivo, 0) + COALESCE(valor_ajuste, 0)) - valor_pago
    ) > 0.01 * valor_pago
"""

TESTES_WARN = {
    "assert_empenho_negativo_monitorado": QUERY_EMPENHO_NEGATIVO,
    "assert_pagamento_dentro_do_contratado": QUERY_PAGAMENTO_ACIMA_CONTRATADO,
}

DDL = f"""
CREATE TABLE IF NOT EXISTS {HEALTH_TABLE} (
    nome_teste varchar,
    n_casos integer,
    coletado_em timestamp
)
"""


def coletar() -> dict[str, int]:
    """Roda cada query espelhada e devolve `{nome_do_teste: n_casos}`."""
    resultado = {}
    for nome, query in TESTES_WARN.items():
        df = trino_io.query(query)
        resultado[nome] = int(df.iloc[0]["n"]) if not df.empty else 0
    return resultado


def persistir(resultado: dict[str, int]) -> None:
    coletado_em = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    payload = pd.DataFrame(
        [{"nome_teste": nome, "n_casos": n, "coletado_em": coletado_em} for nome, n in resultado.items()]
    )
    trino_io.execute(DDL)
    trino_io.bulk_insert(HEALTH_TABLE, payload, list(payload.columns), casts={"coletado_em": "TIMESTAMP"})
    logger.info("Histórico de testes WARN gravado em %s: %s", HEALTH_TABLE, resultado)


def run(persist: bool = True) -> dict[str, int]:
    resultado = coletar()
    logger.info("Testes WARN coletados: %s", resultado)
    if persist:
        persistir(resultado)
    return resultado


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coleta histórico dos testes dbt WARN")
    parser.add_argument("--no-persist", dest="persist", action="store_false")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    print(run(persist=args.persist))
