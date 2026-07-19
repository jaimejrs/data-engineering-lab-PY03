"""Testes do validador de schema/completude da camada Bronze."""

from unittest.mock import patch

import pytest

from src.validators import bronze_validator


class TestValidateSource:
    def test_rejects_source_out_of_scope(self):
        with pytest.raises(ValueError):
            bronze_validator.validate_source("tabela_inexistente", "2026-07-15")

    def test_fails_when_no_partitions_found(self):
        with patch.object(bronze_validator, "find_data_extracao_dirs", return_value=[]):
            with pytest.raises(bronze_validator.BronzeValidationError, match="nenhum arquivo"):
                bronze_validator.validate_source("empenhos", "2026-07-15")

    def test_fails_when_partitions_found_but_empty(self):
        # diretório ano=/mes=/data_extracao= existe, mas sem nenhum chunk .json dentro
        with patch.object(bronze_validator, "find_data_extracao_dirs",
                           return_value=["empenhos/ano=2026/mes=07/data_extracao=2026-07-15"]), \
                patch.object(bronze_validator, "list_json_files", return_value=[]):
            with pytest.raises(bronze_validator.BronzeValidationError, match="nenhum arquivo"):
                bronze_validator.validate_source("empenhos", "2026-07-15")

    def test_fails_when_required_column_missing(self):
        records = [{"id": 1, "ano": 2026}]  # falta 'dataemissao'
        with patch.object(bronze_validator, "find_data_extracao_dirs",
                           return_value=["empenhos/ano=2026/mes=07/data_extracao=2026-07-15"]), \
                patch.object(bronze_validator, "list_json_files",
                             return_value=["empenhos/ano=2026/mes=07/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=records):
            with pytest.raises(bronze_validator.BronzeValidationError, match="colunas obrigatórias ausentes"):
                bronze_validator.validate_source("empenhos", "2026-07-15")

    def test_fails_when_below_minimum_records(self):
        records = [{"id": 1, "ano": 2026, "dataemissao": "2026-07-15"}]
        with patch.object(bronze_validator, "find_data_extracao_dirs",
                           return_value=["empenhos/ano=2026/mes=07/data_extracao=2026-07-15"]), \
                patch.object(bronze_validator, "list_json_files",
                             return_value=["empenhos/ano=2026/mes=07/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=records):
            with pytest.raises(bronze_validator.BronzeValidationError, match="esperado >= 5"):
                bronze_validator.validate_source("empenhos", "2026-07-15", min_records=5)

    def test_passes_with_valid_records(self):
        records = [{"id": 1, "ano": 2026, "dataemissao": "2026-07-15"}, {"id": 2, "ano": 2026, "dataemissao": "2026-07-15"}]
        with patch.object(bronze_validator, "find_data_extracao_dirs",
                           return_value=["empenhos/ano=2026/mes=07/data_extracao=2026-07-15"]), \
                patch.object(bronze_validator, "list_json_files",
                             return_value=["empenhos/ano=2026/mes=07/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=records):
            result = bronze_validator.validate_source("empenhos", "2026-07-15")

        assert result == {"source": "empenhos", "partitions": 1, "files": 1, "records": 2}

    def test_aggregates_records_across_multiple_ano_mes_partitions(self):
        # o mesmo data_extracao pode se espalhar por vários ano=/mes= quando o
        # intervalo incremental cruza uma virada de mês (o caso que a Nara reportou).
        partitions = [
            "empenhos/ano=2026/mes=06/data_extracao=2026-07-15",
            "empenhos/ano=2026/mes=07/data_extracao=2026-07-15",
        ]
        files_by_partition = {
            partitions[0]: [f"{partitions[0]}/chunk_0001.json"],
            partitions[1]: [f"{partitions[1]}/chunk_0001.json"],
        }
        records_by_file = {
            f"{partitions[0]}/chunk_0001.json": [{"id": 1, "ano": 2026, "dataemissao": "2026-06-30"}],
            f"{partitions[1]}/chunk_0001.json": [
                {"id": 2, "ano": 2026, "dataemissao": "2026-07-01"},
                {"id": 3, "ano": 2026, "dataemissao": "2026-07-15"},
            ],
        }

        with patch.object(bronze_validator, "find_data_extracao_dirs", return_value=partitions), \
                patch.object(bronze_validator, "list_json_files", side_effect=lambda d: files_by_partition[d]), \
                patch.object(bronze_validator, "read_json_records", side_effect=lambda f: records_by_file[f]):
            result = bronze_validator.validate_source("empenhos", "2026-07-15")

        assert result == {"source": "empenhos", "partitions": 2, "files": 2, "records": 3}

    def test_allows_empty_reference_table_below_default_minimum(self):
        with patch.object(bronze_validator, "find_data_extracao_dirs",
                           return_value=["unidade_gestora/data_extracao=2026-07-15"]), \
                patch.object(bronze_validator, "list_json_files",
                             return_value=["unidade_gestora/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(bronze_validator, "read_json_records", return_value=[]):
            result = bronze_validator.validate_source("unidade_gestora", "2026-07-15")

        assert result == {"source": "unidade_gestora", "partitions": 1, "files": 1, "records": 0}


class TestValidateBronze:
    def test_validates_all_sources_and_returns_summary(self):
        def fake_find_dirs(source, run_date):
            return [f"{source}/data_extracao={run_date}"]

        def fake_list(relative_dir):
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

        with patch.object(bronze_validator, "find_data_extracao_dirs", side_effect=fake_find_dirs), \
                patch.object(bronze_validator, "list_json_files", side_effect=fake_list), \
                patch.object(bronze_validator, "read_json_records", side_effect=fake_read):
            result = bronze_validator.validate_bronze("2026-07-15")

        assert set(result.keys()) == {"empenhos", "ordem_bancaria_orcamentaria", "unidade_gestora", "contratos"}
        assert all(v["records"] == 1 for v in result.values())

    def test_stops_at_first_invalid_source(self):
        with patch.object(bronze_validator, "find_data_extracao_dirs", return_value=[]):
            with pytest.raises(bronze_validator.BronzeValidationError):
                bronze_validator.validate_bronze("2026-07-15")
