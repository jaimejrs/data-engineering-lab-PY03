"""Testes do enriquecimento de empenhos/OB com unidade_gestora (tarefa 17)."""

import pandas as pd

from src.transformers.enrichment import enrich_with_unidade_gestora


def test_joins_by_codigoug_and_ano():
    empenhos = pd.DataFrame([
        {"id": 1, "ano": 2026, "codigoug": "100", "valor": 500.0},
        {"id": 2, "ano": 2025, "codigoug": "100", "valor": 300.0},  # mesmo codigo, ano diferente
    ])
    unidade_gestora = pd.DataFrame([
        {"codigo": "100", "ano": 2026, "titulo": "Secretaria X", "sigla": "SX"},
        {"codigo": "100", "ano": 2025, "titulo": "Secretaria X (antiga)", "sigla": "SXA"},
    ])

    result = enrich_with_unidade_gestora(empenhos, unidade_gestora)

    row_2026 = result[result["id"] == 1].iloc[0]
    row_2025 = result[result["id"] == 2].iloc[0]
    assert row_2026["orgao_nome"] == "Secretaria X"
    assert row_2025["orgao_nome"] == "Secretaria X (antiga)"


def test_keeps_record_without_matching_orgao():
    empenhos = pd.DataFrame([{"id": 1, "ano": 2026, "codigoug": "999", "valor": 100.0}])
    unidade_gestora = pd.DataFrame([{"codigo": "100", "ano": 2026, "titulo": "Secretaria X"}])

    result = enrich_with_unidade_gestora(empenhos, unidade_gestora)

    assert len(result) == 1
    assert pd.isna(result.iloc[0]["orgao_nome"])


def test_empty_records_returns_empty():
    empenhos = pd.DataFrame()
    unidade_gestora = pd.DataFrame([{"codigo": "100", "ano": 2026, "titulo": "Secretaria X"}])

    result = enrich_with_unidade_gestora(empenhos, unidade_gestora)

    assert result.empty
