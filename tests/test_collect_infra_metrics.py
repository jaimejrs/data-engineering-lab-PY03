"""Testes de `deploy/server-lakehouse/collect_infra_metrics.py` — coleta de
métricas de infra (CPU/memória/disco) pro painel "Métricas de
Infraestrutura" no Superset. Carregado via `importlib` (script standalone
rodado por cron no servidor, igual auto-sync.py). `run()` é sempre mockado —
nenhum teste aqui toca Docker nem o Trino real.
"""

import importlib.util
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "deploy" / "server-lakehouse" / "collect_infra_metrics.py"


@pytest.fixture
def mod():
    spec = importlib.util.spec_from_file_location("collect_infra_metrics", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _completed(stdout=""):
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


class TestParseSize:
    def test_gib_to_bytes(self, mod):
        assert mod.parse_size("1GiB") == 1024**3

    def test_decimal_value(self, mod):
        assert mod.parse_size("1.5MiB") == int(1.5 * 1024**2)

    def test_mb_treated_as_binary_like_docker(self, mod):
        # Docker mistura sufixo "MB"/"GB" com valor em base 1024 dependendo da
        # versão — tratamos os dois como base 1024 (ver comentário no módulo).
        assert mod.parse_size("2GB") == 2 * 1024**3

    def test_invalid_text_is_zero(self, mod):
        assert mod.parse_size("") == 0
        assert mod.parse_size("garbage") == 0


class TestParsePercent:
    def test_strips_percent_sign(self, mod):
        assert mod.parse_percent("12.34%") == 12.34

    def test_empty_is_zero(self, mod):
        assert mod.parse_percent("") == 0.0

    def test_non_numeric_raises(self, mod):
        with pytest.raises(ValueError):
            mod.parse_percent("--")


class TestCollectContainerStats:
    def test_parses_one_json_line_per_container(self, mod):
        stdout = (
            '{"Name":"lakehouse_trino","CPUPerc":"3.50%","MemUsage":"512MiB / 6GiB","MemPerc":"8.33%"}\n'
            '{"Name":"lakehouse_spark_master","CPUPerc":"0.10%","MemUsage":"1GiB / 8GiB","MemPerc":"12.50%"}\n'
        )
        with patch.object(mod, "run", return_value=_completed(stdout)):
            rows = mod.collect_container_stats()
        assert len(rows) == 2
        assert rows[0]["container"] == "lakehouse_trino"
        assert rows[0]["cpu_percent"] == 3.50
        assert rows[0]["mem_usage_bytes"] == 512 * 1024**2
        assert rows[0]["mem_limit_bytes"] == 6 * 1024**3
        assert rows[0]["mem_percent"] == 8.33

    def test_empty_output_is_empty_list(self, mod):
        with patch.object(mod, "run", return_value=_completed("")):
            assert mod.collect_container_stats() == []


class TestCollectDiskStats:
    def test_parses_df_output_skipping_header(self, mod):
        stdout = (
            "Mounted on            1B-blocks         Used Use%\n"
            "/              500000000000  250000000000  50%\n"
            "/data          200000000000   40000000000  20%\n"
        )
        with patch.object(mod, "run", return_value=_completed(stdout)):
            rows = mod.collect_disk_stats()
        assert len(rows) == 2
        assert rows[0]["ponto_montagem"] == "/"
        assert rows[0]["total_bytes"] == 500000000000
        assert rows[0]["usado_bytes"] == 250000000000
        assert rows[0]["pct_usado"] == 50.0
        assert rows[1]["ponto_montagem"] == "/data"

    def test_malformed_lines_are_skipped(self, mod):
        stdout = "Mounted on 1B-blocks Used Use%\nsomething malformed here that has extra fields 1 2 3\n"
        with patch.object(mod, "run", return_value=_completed(stdout)):
            assert mod.collect_disk_stats() == []

    def test_tiny_pseudo_filesystems_are_filtered_out(self, mod):
        stdout = (
            "Mounted on 1B-blocks Used Use%\n"
            "/sys/firmware/efi/efivars 188328 157343 86%\n"
            "/ 500000000000 250000000000 50%\n"
        )
        with patch.object(mod, "run", return_value=_completed(stdout)):
            rows = mod.collect_disk_stats()
        assert [r["ponto_montagem"] for r in rows] == ["/"]


class TestBuildInsertSql:
    def test_build_insert_containers_has_one_tuple_per_row(self, mod):
        rows = [
            {"container": "a", "cpu_percent": 1.0, "mem_usage_bytes": 10, "mem_limit_bytes": 100, "mem_percent": 10.0},
            {"container": "b", "cpu_percent": 2.0, "mem_usage_bytes": 20, "mem_limit_bytes": 200, "mem_percent": 10.0},
        ]
        sql = mod.build_insert_containers(rows, "2026-07-25 12:00:00")
        assert sql.count("(") == 2
        assert "'a'" in sql and "'b'" in sql
        assert "infra_metricas_containers" in sql

    def test_build_insert_disco_has_one_tuple_per_row(self, mod):
        rows = [{"ponto_montagem": "/", "total_bytes": 100, "usado_bytes": 50, "pct_usado": 50.0}]
        sql = mod.build_insert_disco(rows, "2026-07-25 12:00:00")
        assert "infra_metricas_disco" in sql
        assert "'/'" in sql


class TestMain:
    def test_main_creates_tables_and_inserts_when_data_present(self, mod):
        calls = []

        def fake_run(args):
            calls.append(args)
            if args[:2] == ["docker", "stats"]:
                return _completed('{"Name":"c1","CPUPerc":"1.00%","MemUsage":"1GiB / 2GiB","MemPerc":"50.00%"}\n')
            if args[0] == "df":
                return _completed("Mounted on 1B-blocks Used Use%\n/ 500000000000 250000000000 50%\n")
            return _completed("")

        with patch.object(mod, "run", side_effect=fake_run):
            mod.main("fake_trino_container")

        exec_calls = [c for c in calls if c[:2] == ["docker", "exec"]]
        # 2 DDLs + 2 INSERTs (containers e disco)
        assert len(exec_calls) == 4
        assert any("CREATE TABLE" in c[-1] for c in exec_calls)
        assert any("INSERT INTO" in c[-1] and "infra_metricas_containers" in c[-1] for c in exec_calls)
        assert any("INSERT INTO" in c[-1] and "infra_metricas_disco" in c[-1] for c in exec_calls)

    def test_main_skips_insert_when_no_rows(self, mod):
        calls = []

        def fake_run(args):
            calls.append(args)
            return _completed("")

        with patch.object(mod, "run", side_effect=fake_run):
            mod.main("fake_trino_container")

        exec_calls = [c for c in calls if c[:2] == ["docker", "exec"]]
        # só as 2 DDLs, nenhum INSERT (sem containers/discos coletados)
        assert len(exec_calls) == 2
