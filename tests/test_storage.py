"""Testes da camada de armazenamento da Bronze (backend local)."""

from src.extractors import storage


class TestFindDataExtracaoDirs:
    def test_finds_flat_layout(self, tmp_path, monkeypatch):
        # contratos/unidade_gestora: data_extracao= direto sob a raiz da fonte.
        monkeypatch.setattr(storage, "BRONZE_BASE_PATH", str(tmp_path))
        (tmp_path / "contratos" / "data_extracao=2026-07-15").mkdir(parents=True)

        result = storage.find_data_extracao_dirs("contratos", "2026-07-15")

        assert result == ["contratos/data_extracao=2026-07-15"]

    def test_finds_nested_ano_mes_layout(self, tmp_path, monkeypatch):
        # empenhos/ordem_bancaria_orcamentaria: particionado por ano=/mes=,
        # e um mesmo data_extracao pode se espalhar por vários meses.
        monkeypatch.setattr(storage, "BRONZE_BASE_PATH", str(tmp_path))
        (tmp_path / "empenhos" / "ano=2026" / "mes=06" / "data_extracao=2026-07-15").mkdir(parents=True)
        (tmp_path / "empenhos" / "ano=2026" / "mes=07" / "data_extracao=2026-07-15").mkdir(parents=True)

        result = storage.find_data_extracao_dirs("empenhos", "2026-07-15")

        assert result == [
            "empenhos/ano=2026/mes=06/data_extracao=2026-07-15",
            "empenhos/ano=2026/mes=07/data_extracao=2026-07-15",
        ]

    def test_ignores_other_run_dates(self, tmp_path, monkeypatch):
        monkeypatch.setattr(storage, "BRONZE_BASE_PATH", str(tmp_path))
        (tmp_path / "empenhos" / "ano=2026" / "mes=07" / "data_extracao=2026-07-14").mkdir(parents=True)

        result = storage.find_data_extracao_dirs("empenhos", "2026-07-15")

        assert result == []

    def test_returns_empty_when_source_root_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(storage, "BRONZE_BASE_PATH", str(tmp_path))

        result = storage.find_data_extracao_dirs("empenhos", "2026-07-15")

        assert result == []

    def test_result_is_directly_usable_by_list_json_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(storage, "BRONZE_BASE_PATH", str(tmp_path))
        partition = tmp_path / "empenhos" / "ano=2026" / "mes=07" / "data_extracao=2026-07-15"
        partition.mkdir(parents=True)
        (partition / "chunk_0001.json").write_text("[]", encoding="utf-8")

        dirs = storage.find_data_extracao_dirs("empenhos", "2026-07-15")
        files = [f for d in dirs for f in storage.list_json_files(d)]

        assert files == ["empenhos/ano=2026/mes=07/data_extracao=2026-07-15/chunk_0001.json"]
