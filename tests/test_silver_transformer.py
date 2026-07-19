"""Testes da transformação Bronze -> Silver (normalização, dedup, particionamento)."""

from unittest.mock import patch

import pytest

from src.transformers import silver_transformer


class TestTransformRecord:
    def test_normalizes_api_date_fields(self):
        record = {"id": 1, "data_assinatura": "15/07/2026", "data_rescisao": None}
        result = silver_transformer.transform_record("contratos", record)
        assert result["data_assinatura"] == "2026-07-15"
        assert result["data_rescisao"] is None

    def test_normalizes_postgres_date_field(self):
        record = {"id": 1, "ano": 2026, "dataemissao": "2026-07-15 00:00:00.000"}
        result = silver_transformer.transform_record("empenhos", record)
        assert result["dataemissao"] == "2026-07-15"

    def test_keeps_invalid_api_date_unchanged_with_warning(self):
        record = {"id": 1, "data_assinatura": "2026-07-15"}  # já ISO, não é DD/MM/YYYY
        result = silver_transformer.transform_record("contratos", record)
        assert result["data_assinatura"] == "2026-07-15"

    def test_normalizes_cnpj_cpf_preferring_plain_field(self):
        record = {
            "id": 1,
            "plain_cpf_cnpj_financiador": "12345678901",
            "cpf_cnpj_financiador": "123.456.789-01",
        }
        result = silver_transformer.transform_record("contratos", record)
        assert result["cnpj_cpf_normalizado"] == "12345678901"
        assert result["tipo_credor"] == "PF"

    def test_falls_back_to_masked_field_when_plain_missing(self):
        record = {"id": 1, "cpf_cnpj_financiador": "12.345.678/0001-95"}
        result = silver_transformer.transform_record("contratos", record)
        assert result["cnpj_cpf_normalizado"] == "12345678000195"
        assert result["tipo_credor"] == "PJ"

    def test_flags_invalid_length_without_dropping_record(self):
        record = {"id": 1, "cpf_cnpj_financiador": "123"}
        result = silver_transformer.transform_record("contratos", record)
        assert result["cnpj_cpf_normalizado"] == "123"
        assert result["tipo_credor"] == "INVALIDO"

    def test_does_not_add_cnpj_fields_for_other_sources(self):
        record = {"id": 1, "ano": 2026, "dataemissao": "2026-07-15 00:00:00.000"}
        result = silver_transformer.transform_record("empenhos", record)
        assert "cnpj_cpf_normalizado" not in result

    def test_does_not_mutate_input_record(self):
        record = {"id": 1, "data_assinatura": "15/07/2026"}
        silver_transformer.transform_record("contratos", record)
        assert record["data_assinatura"] == "15/07/2026"


class TestDedup:
    def test_removes_duplicate_key_keeping_last_occurrence(self):
        records = [
            {"id": 1, "ano": 2026, "dataemissao": "2026-07-01"},
            {"id": 1, "ano": 2026, "dataemissao": "2026-07-15"},  # mesma chave (id, ano), reaparece
            {"id": 2, "ano": 2026, "dataemissao": "2026-07-10"},
        ]
        result = silver_transformer._dedup("empenhos", records)
        assert len(result) == 2
        kept = next(r for r in result if r["id"] == 1)
        assert kept["dataemissao"] == "2026-07-15"

    def test_no_duplicates_returns_all(self):
        records = [{"id": 1, "ano": 2026}, {"id": 2, "ano": 2026}]
        result = silver_transformer._dedup("empenhos", records)
        assert len(result) == 2


class TestTransformSource:
    def test_rejects_source_out_of_scope(self):
        with pytest.raises(ValueError):
            silver_transformer.transform_source("tabela_inexistente", "2026-07-15")

    def test_writes_one_parquet_per_ano_mes_partition(self):
        records = [
            {"id": 1, "ano": 2026, "dataemissao": "2026-06-30 00:00:00.000"},
            {"id": 2, "ano": 2026, "dataemissao": "2026-07-01 00:00:00.000"},
        ]
        written = []

        def fake_write(relative_path, group_records):
            written.append((relative_path, group_records))
            return relative_path

        with patch.object(silver_transformer, "find_data_extracao_dirs",
                           return_value=["empenhos/ano=2026/mes=06/data_extracao=2026-07-15",
                                         "empenhos/ano=2026/mes=07/data_extracao=2026-07-15"]), \
                patch.object(silver_transformer, "list_json_files",
                             return_value=["chunk_0001.json"]), \
                patch.object(silver_transformer, "read_json_records",
                             side_effect=[[records[0]], [records[1]]]), \
                patch.object(silver_transformer, "write_parquet_records", side_effect=fake_write):
            result = silver_transformer.transform_source("empenhos", "2026-07-15")

        assert result == {
            "source": "empenhos",
            "bronze_files": 2,
            "records_read": 2,
            "records_written": 2,
            "silver_files": 2,
        }
        paths = sorted(path for path, _ in written)
        assert paths == [
            "empenhos/ano=2026/mes=06/data_extracao=2026-07-15/part_0001.parquet",
            "empenhos/ano=2026/mes=07/data_extracao=2026-07-15/part_0001.parquet",
        ]

    def test_writes_flat_path_for_source_without_partition_field(self):
        with patch.object(silver_transformer, "find_data_extracao_dirs",
                           return_value=["unidade_gestora/data_extracao=2026-07-15"]), \
                patch.object(silver_transformer, "list_json_files",
                             return_value=["unidade_gestora/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(silver_transformer, "read_json_records",
                             return_value=[{"codigo": "1", "ano": 2026}]), \
                patch.object(silver_transformer, "write_parquet_records") as mock_write:
            result = silver_transformer.transform_source("unidade_gestora", "2026-07-15")

        mock_write.assert_called_once_with(
            "unidade_gestora/data_extracao=2026-07-15/part_0001.parquet", [{"codigo": "1", "ano": 2026}]
        )
        assert result["silver_files"] == 1

    def test_writes_nothing_when_no_records(self):
        with patch.object(silver_transformer, "find_data_extracao_dirs",
                           return_value=["unidade_gestora/data_extracao=2026-07-15"]), \
                patch.object(silver_transformer, "list_json_files",
                             return_value=["unidade_gestora/data_extracao=2026-07-15/chunk_0001.json"]), \
                patch.object(silver_transformer, "read_json_records", return_value=[]), \
                patch.object(silver_transformer, "write_parquet_records") as mock_write:
            result = silver_transformer.transform_source("unidade_gestora", "2026-07-15")

        mock_write.assert_not_called()
        assert result == {
            "source": "unidade_gestora",
            "bronze_files": 1,
            "records_read": 0,
            "records_written": 0,
            "silver_files": 0,
        }


class TestTransformBronzeToSilver:
    def test_transforms_all_sources(self):
        with patch.object(silver_transformer, "transform_source") as mock_transform:
            mock_transform.side_effect = lambda source, run_date: {"source": source}
            result = silver_transformer.transform_bronze_to_silver("2026-07-15")

        assert set(result.keys()) == {"empenhos", "ordem_bancaria_orcamentaria", "unidade_gestora", "contratos"}
        assert mock_transform.call_count == 4
