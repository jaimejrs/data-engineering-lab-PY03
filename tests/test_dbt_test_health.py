"""Testes de `src/dbt_test_health.py` — rastreamento histórico dos testes dbt
WARN (auditoria de rigor científico de 30/07/2026). Sem tocar Trino/rede: as
queries embutidas são testadas só quanto ao formato (mesmo padrão de
`test_payment_forecast.py`/`test_anomaly_detection.py` para
`PAGAMENTO_QUERY`/`FEATURE_QUERY`).
"""

from unittest.mock import patch

import pandas as pd

from src import dbt_test_health as dth


def _df_n(n: int) -> pd.DataFrame:
    return pd.DataFrame({"n": [n]})


class TestColetar:
    def test_returns_one_count_per_teste_warn(self):
        with patch.object(dth.trino_io, "query", side_effect=[_df_n(1), _df_n(919)]):
            resultado = dth.coletar()
        assert resultado == {
            "assert_empenho_negativo_monitorado": 1,
            "assert_pagamento_dentro_do_contratado": 919,
        }

    def test_empty_result_becomes_zero(self):
        with patch.object(dth.trino_io, "query", return_value=pd.DataFrame(columns=["n"])):
            resultado = dth.coletar()
        assert all(n == 0 for n in resultado.values())


class TestPersistir:
    def test_creates_table_and_inserts_one_row_per_teste(self):
        with (
            patch.object(dth.trino_io, "execute") as mock_execute,
            patch.object(dth.trino_io, "bulk_insert") as mock_bulk_insert,
        ):
            dth.persistir({"assert_empenho_negativo_monitorado": 1, "assert_pagamento_dentro_do_contratado": 919})

        mock_execute.assert_called_once_with(dth.DDL)
        mock_bulk_insert.assert_called_once()
        args, kwargs = mock_bulk_insert.call_args
        table, payload, columns = args[0], args[1], args[2]
        assert table == dth.HEALTH_TABLE
        assert set(payload["nome_teste"]) == {
            "assert_empenho_negativo_monitorado",
            "assert_pagamento_dentro_do_contratado",
        }
        assert list(payload.loc[payload["nome_teste"] == "assert_pagamento_dentro_do_contratado", "n_casos"]) == [919]
        assert columns == ["nome_teste", "n_casos", "coletado_em"]
        assert kwargs["casts"] == {"coletado_em": "TIMESTAMP"}


class TestRun:
    def test_persist_true_calls_persistir(self):
        with (
            patch.object(dth, "coletar", return_value={"a": 1}),
            patch.object(dth, "persistir") as mock_persistir,
        ):
            resultado = dth.run(persist=True)
        mock_persistir.assert_called_once_with({"a": 1})
        assert resultado == {"a": 1}

    def test_persist_false_skips_persistir(self):
        with (
            patch.object(dth, "coletar", return_value={"a": 1}),
            patch.object(dth, "persistir") as mock_persistir,
        ):
            dth.run(persist=False)
        mock_persistir.assert_not_called()


class TestQueries:
    def test_query_empenho_negativo_mirrors_dbt_test_condition(self):
        assert "valor_empenhado < 0" in dth.QUERY_EMPENHO_NEGATIVO
        assert "iceberg.gold.fato_contrato" in dth.QUERY_EMPENHO_NEGATIVO

    def test_query_pagamento_acima_contratado_mirrors_dbt_test_condition(self):
        q = dth.QUERY_PAGAMENTO_ACIMA_CONTRATADO
        assert "valor_contrato * 2" in q
        assert "valor_aditivo" in q and "valor_ajuste" in q
        assert "0.01 * valor_pago" in q
