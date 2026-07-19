"""Testes dos extractors de ingestão (API Ceará Transparente e PostgreSQL)."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.extractors import api_extractor, postgres_extractor


def _fake_response(payload, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


class TestFetchContratos:
    def test_stops_at_total_pages(self):
        session = MagicMock()
        session.get.side_effect = [
            _fake_response({"summary": {"total_pages": 2}, "data": [{"id": 1}]}),
            _fake_response({"summary": {"total_pages": 2}, "data": [{"id": 2}]}),
        ]

        with patch.object(api_extractor.time, "sleep"):
            pages = list(api_extractor.fetch_contratos(session=session))

        assert [p for p, _ in pages] == [1, 2]
        assert session.get.call_count == 2

    def test_missing_total_pages_raises_instead_of_looping(self):
        session = MagicMock()
        session.get.return_value = _fake_response({"data": [{"id": 1}]})

        with pytest.raises(api_extractor.CearaTransparenteAPIError):
            list(api_extractor.fetch_contratos(session=session))

        # nunca deve tentar uma segunda página sem saber o total
        assert session.get.call_count == 1

    def test_single_page_does_not_sleep(self):
        session = MagicMock()
        session.get.return_value = _fake_response({"summary": {"total_pages": 1}, "data": []})

        with patch.object(api_extractor.time, "sleep") as mock_sleep:
            list(api_extractor.fetch_contratos(session=session))

        mock_sleep.assert_not_called()


class TestExtractAndSave:
    def test_writes_each_page_and_returns_counts(self):
        session = MagicMock()
        session.get.side_effect = [
            _fake_response({"summary": {"total_pages": 2}, "data": [
                {"id": 1, "data_assinatura": "2026-01-05T00:00:00.000-03:00"},
                {"id": 2, "data_assinatura": "2026-03-10T00:00:00.000-03:00"},
            ]}),
            _fake_response({"summary": {"total_pages": 2}, "data": [
                {"id": 3, "data_assinatura": "2026-02-01T00:00:00.000-03:00"},
            ]}),
        ]

        with patch.object(api_extractor, "write_json_records") as mock_write, \
                patch.object(api_extractor, "fetch_contratos", wraps=api_extractor.fetch_contratos) as _, \
                patch("requests.Session", return_value=session), \
                patch.object(api_extractor.time, "sleep"):
            result = api_extractor.extract_and_save(run_date="2026-07-15")

        assert result == {
            "total_pages": 2,
            "total_records": 3,
            "run_date": "2026-07-15",
            "max_data_assinatura": "2026-03-10",
        }
        # página 1 tem registros de jan/2026 e mar/2026 (2 arquivos), página 2 é
        # só fev/2026 (1 arquivo) — 3 arquivos particionados por ano/mes no total.
        assert mock_write.call_count == 3
        written_paths = {call.args[0] for call in mock_write.call_args_list}
        assert written_paths == {
            "contratos/ano=2026/mes=01/data_extracao=2026-07-15/page_0001.json",
            "contratos/ano=2026/mes=03/data_extracao=2026-07-15/page_0001.json",
            "contratos/ano=2026/mes=02/data_extracao=2026-07-15/page_0001.json",
        }


class TestBuildQuery:
    def test_no_filters_returns_plain_select(self):
        query, params = postgres_extractor._build_query("unidade_gestora", None, None, None)
        assert str(query) == "SELECT * FROM unidade_gestora"
        assert params == {}

    def test_filters_applied_when_date_column_present(self):
        query, params = postgres_extractor._build_query(
            "empenhos", "data_empenho", "2026-01-01", "2026-01-31"
        )
        assert "WHERE" in str(query)
        assert "data_empenho >= :data_inicio" in str(query)
        assert "data_empenho <= :data_fim" in str(query)
        assert params == {"data_inicio": "2026-01-01", "data_fim": "2026-01-31"}


class TestExtractTable:
    def test_rejects_table_out_of_scope(self):
        with pytest.raises(ValueError):
            postgres_extractor.extract_table("tabela_inexistente")


class TestExtractTableChunks:
    def test_rejects_table_out_of_scope(self):
        with pytest.raises(ValueError):
            list(postgres_extractor.extract_table_chunks("tabela_inexistente"))

    def test_yields_each_chunk_from_read_sql(self):
        chunks = [pd.DataFrame({"id": [1, 2]}), pd.DataFrame({"id": [3]})]
        engine = MagicMock()

        with patch.object(postgres_extractor.pd, "read_sql", return_value=iter(chunks)):
            result = list(postgres_extractor.extract_table_chunks("empenhos", engine=engine))

        assert [len(c) for c in result] == [2, 1]


class TestExtractAndSaveChunked:
    def _fake_read_sql(self, chunks_by_table):
        def _read_sql(query, engine, params=None, chunksize=None):
            for table, chunks in chunks_by_table.items():
                if table in str(query):
                    return iter(chunks)
            raise AssertionError(f"query inesperada: {query}")

        return _read_sql

    def test_writes_one_file_per_chunk_and_sums_counts(self):
        chunks_by_table = {
            "empenhos": [pd.DataFrame({"id": [1, 2]}), pd.DataFrame({"id": [3]})],
            "ordem_bancaria_orcamentaria": [pd.DataFrame({"id": [10]})],
            "unidade_gestora": [pd.DataFrame({"id": [100, 101]})],
        }

        with patch.object(postgres_extractor, "write_json_records") as mock_write, \
                patch.object(postgres_extractor.pd, "read_sql", side_effect=self._fake_read_sql(chunks_by_table)), \
                patch.object(postgres_extractor, "create_engine", return_value=MagicMock()):
            result = postgres_extractor.extract_and_save(run_date="2026-07-15")

        assert result["counts"] == {
            "empenhos": 3,
            "ordem_bancaria_orcamentaria": 1,
            "unidade_gestora": 2,
        }
        # 2 chunks (empenhos) + 1 (OB) + 1 (UG) = 4 arquivos gravados
        assert mock_write.call_count == 4
        first_call_path = mock_write.call_args_list[0].args[0]
        assert first_call_path == "empenhos/data_extracao=2026-07-15/chunk_0001.json"

    def test_partitions_by_ano_mes_from_date_column(self):
        chunks_by_table = {
            "empenhos": [pd.DataFrame({
                "id": [1, 2, 3],
                "dataemissao": [
                    "2022-01-10 00:00:00.000",
                    "2022-01-20 00:00:00.000",
                    "2022-02-05 00:00:00.000",
                ],
            })],
            "ordem_bancaria_orcamentaria": [pd.DataFrame({"id": [], "dataemissao": []})],
            "unidade_gestora": [pd.DataFrame({"id": [100]})],
        }

        with patch.object(postgres_extractor, "write_json_records") as mock_write, \
                patch.object(postgres_extractor.pd, "read_sql", side_effect=self._fake_read_sql(chunks_by_table)), \
                patch.object(postgres_extractor, "create_engine", return_value=MagicMock()):
            result = postgres_extractor.extract_and_save(run_date="2026-07-15")

        assert result["counts"]["empenhos"] == 3
        assert result["max_dates"]["empenhos"] == "2022-02-05"
        assert result["max_dates"]["unidade_gestora"] is None

        written_paths = {call.args[0] for call in mock_write.call_args_list if "empenhos" in call.args[0]}
        assert written_paths == {
            "empenhos/ano=2022/mes=01/data_extracao=2026-07-15/chunk_0001.json",
            "empenhos/ano=2022/mes=02/data_extracao=2026-07-15/chunk_0001.json",
        }

    def test_writes_single_empty_marker_when_no_rows_match(self):
        chunks_by_table = {
            "empenhos": [],
            "ordem_bancaria_orcamentaria": [],
            "unidade_gestora": [],
        }

        with patch.object(postgres_extractor, "write_json_records") as mock_write, \
                patch.object(postgres_extractor.pd, "read_sql", side_effect=self._fake_read_sql(chunks_by_table)), \
                patch.object(postgres_extractor, "create_engine", return_value=MagicMock()):
            result = postgres_extractor.extract_and_save(run_date="2026-07-15")

        assert result["counts"] == {"empenhos": 0, "ordem_bancaria_orcamentaria": 0, "unidade_gestora": 0}
        assert mock_write.call_count == 3
        for call in mock_write.call_args_list:
            assert call.args[0].endswith("chunk_0001.json")
            assert call.args[1] == []
