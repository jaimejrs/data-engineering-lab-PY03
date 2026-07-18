"""Testes do validador de schema/completude da camada Bronze."""

from unittest.mock import patch

import pytest

from src.validators import bronze_validator


class TestValidateSource:
    def test_rejects_source_out_of_scope(self):
        with pytest.raises(ValueError):
            bronze_validator.validate_source("tabela_inexistente", "2026-07-15")

    def test_fails_when_no_files_found(self):
        with patch.object(bronze_validator, "list_json_files", return_value=[]):
            with pytest.raises(bronze_validator.BronzeValidationError, match="nenhum arquivo"):
                bronze_validator.validate_source("empenhos", "2026-07-15")

    def test_fails_when_required_column_missing(self):
        records = [{"id": 1, "ano": 2026}]  # falta 'dataemissao'
        with patch.object(bronze_validator, "list_json_files", return_value=["empenhos/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=records):
            with pytest.raises(bronze_validator.BronzeValidationError, match="colunas obrigatórias ausentes"):
                bronze_validator.validate_source("empenhos", "2026-07-15")

    def test_fails_when_below_minimum_records(self):
        records = [{"id": 1, "ano": 2026, "dataemissao": "2026-07-15"}]
        with patch.object(bronze_validator, "list_json_files", return_value=["empenhos/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=records):
            with pytest.raises(bronze_validator.BronzeValidationError, match="esperado >= 5"):
                bronze_validator.validate_source("empenhos", "2026-07-15", min_records=5)

    def test_passes_with_valid_records(self):
        records = [{"id": 1, "ano": 2026, "dataemissao": "2026-07-15"}, {"id": 2, "ano": 2026, "dataemissao": "2026-07-15"}]
        with patch.object(bronze_validator, "list_json_files", return_value=["empenhos/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=records):
            result = bronze_validator.validate_source("empenhos", "2026-07-15")

        assert result == {"source": "empenhos", "files": 1, "records": 2}

    def test_allows_empty_reference_table_below_default_minimum(self):
        with patch.object(bronze_validator, "list_json_files", return_value=["unidade_gestora/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=[]):
            result = bronze_validator.validate_source("unidade_gestora", "2026-07-15")

        assert result == {"source": "unidade_gestora", "files": 1, "records": 0}


class TestValidateBronze:
    def test_validates_all_sources_and_returns_summary(self):
        def fake_list(relative_dir):
            source = relative_dir.split("/")[0]
            return [f"{relative_dir}/chunk_0001.json"]

        records_by_source = {
            "empenhos": [{"id": 1, "ano": 2026, "dataemissao": "2026-07-15"}],
            "ordem_bancaria_orcamentaria": [{"id": 1, "ano": 2026, "dataemissao": "2026-07-15"}],
            "unidade_gestora": [{"codigo": "1", "ano": 2026}],
            "contratos": [
                {
                    "id": 1,
                    "num_contrato": "123",
                    "valor_contrato": 1000.0,
                    "data_assinatura": "15/07/2026",
                    "cod_gestora": "1",
                }
            ],
        }

        def fake_read(relative_path):
            source = relative_path.split("/")[0]
            return records_by_source[source]

        with patch.object(bronze_validator, "list_json_files", side_effect=fake_list), \
                patch.object(bronze_validator, "read_json_records", side_effect=fake_read):
            result = bronze_validator.validate_bronze("2026-07-15")

        assert set(result.keys()) == {"empenhos", "ordem_bancaria_orcamentaria", "unidade_gestora", "contratos"}
        assert all(v["records"] == 1 for v in result.values())

    def test_stops_at_first_invalid_source(self):
        with patch.object(bronze_validator, "list_json_files", return_value=[]):
            with pytest.raises(bronze_validator.BronzeValidationError):
                bronze_validator.validate_bronze("2026-07-15")
