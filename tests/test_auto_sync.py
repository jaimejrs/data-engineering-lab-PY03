"""Testes de `deploy/server-lakehouse/auto-sync.py` — deploy automático
pull-based (item 12 de docs/06-analise-critica.md). Carregado via
`importlib` (não é um pacote Python, é um script standalone rodado por cron
no servidor). subprocess e chamada HTTP são sempre mockados — nenhum teste
aqui toca rede ou o servidor real.
"""

import importlib.util
import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "deploy" / "server-lakehouse" / "auto-sync.py"


@pytest.fixture
def auto_sync(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("auto_sync", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monkeypatch.setattr(module, "HOME", tmp_path)
    monkeypatch.setattr(module, "SYNC_SCRIPT", tmp_path / "sync-from-git.sh")
    monkeypatch.setattr(module, "STATE_FILE", tmp_path / ".auto-sync-last-sha")
    monkeypatch.setattr(module, "LOG_FILE", tmp_path / "auto-sync.log")
    return module


def _completed(stdout="", stderr="", returncode=0):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


def _check_runs_response(runs):
    body = json.dumps({"check_runs": runs}).encode()
    cm = MagicMock()
    cm.__enter__.return_value = cm
    cm.read.return_value = body
    return cm


class TestRemoteSha:
    def test_parses_sha_from_ls_remote_output(self, auto_sync):
        with patch.object(auto_sync, "run", return_value=_completed(stdout="abc123\trefs/heads/main\n")):
            assert auto_sync.remote_sha() == "abc123"

    def test_returns_none_when_ls_remote_fails(self, auto_sync):
        with patch.object(auto_sync, "run", return_value=_completed(returncode=1, stderr="network unreachable")):
            assert auto_sync.remote_sha() is None


class TestCiPassed:
    def _mock_checks(self, auto_sync, runs):
        return patch.object(
            auto_sync.urllib.request,
            "urlopen",
            return_value=_check_runs_response(runs),
        )

    def test_true_when_all_checks_completed_success(self, auto_sync):
        runs = [
            {"name": "ruff", "status": "completed", "conclusion": "success"},
            {"name": "pytest", "status": "completed", "conclusion": "success"},
        ]
        with self._mock_checks(auto_sync, runs):
            assert auto_sync.ci_passed("sha123") is True

    def test_false_when_any_check_still_running(self, auto_sync):
        runs = [
            {"name": "ruff", "status": "completed", "conclusion": "success"},
            {"name": "pytest", "status": "in_progress", "conclusion": None},
        ]
        with self._mock_checks(auto_sync, runs):
            assert auto_sync.ci_passed("sha123") is False

    def test_false_when_any_check_failed(self, auto_sync):
        runs = [
            {"name": "ruff", "status": "completed", "conclusion": "success"},
            {"name": "pytest", "status": "completed", "conclusion": "failure"},
        ]
        with self._mock_checks(auto_sync, runs):
            assert auto_sync.ci_passed("sha123") is False

    def test_false_when_no_check_runs_yet(self, auto_sync):
        with self._mock_checks(auto_sync, []):
            assert auto_sync.ci_passed("sha123") is False

    def test_false_on_network_error(self, auto_sync):
        with patch.object(auto_sync.urllib.request, "urlopen", side_effect=OSError("boom")):
            assert auto_sync.ci_passed("sha123") is False


class TestMain:
    def test_skips_when_sha_matches_last_deployed(self, auto_sync):
        auto_sync.STATE_FILE.write_text("same-sha")
        with (
            patch.object(auto_sync, "remote_sha", return_value="same-sha"),
            patch.object(auto_sync, "ci_passed") as mock_ci,
            patch.object(auto_sync, "sync") as mock_sync,
        ):
            auto_sync.main()
            mock_ci.assert_not_called()
            mock_sync.assert_not_called()

    def test_does_not_apply_when_ci_not_green(self, auto_sync):
        with (
            patch.object(auto_sync, "remote_sha", return_value="new-sha"),
            patch.object(auto_sync, "ci_passed", return_value=False),
            patch.object(auto_sync, "sync") as mock_sync,
        ):
            auto_sync.main()
            mock_sync.assert_not_called()
        assert not auto_sync.STATE_FILE.exists()

    def test_applies_and_records_state_when_ci_green_and_sync_succeeds(self, auto_sync):
        with (
            patch.object(auto_sync, "remote_sha", return_value="new-sha"),
            patch.object(auto_sync, "ci_passed", return_value=True),
            patch.object(auto_sync, "sync", return_value=_completed(returncode=0)) as mock_sync,
            patch.object(auto_sync, "post_deploy_healthcheck") as mock_health,
        ):
            auto_sync.main()
            assert mock_sync.call_count == 2  # dry-run + --apply
            mock_sync.assert_any_call(apply=False)
            mock_sync.assert_any_call(apply=True)
            mock_health.assert_called_once()
        assert auto_sync.STATE_FILE.read_text() == "new-sha"

    def test_does_not_record_state_when_apply_fails(self, auto_sync):
        results = [_completed(returncode=0), _completed(returncode=1, stderr="rsync error")]
        with (
            patch.object(auto_sync, "remote_sha", return_value="new-sha"),
            patch.object(auto_sync, "ci_passed", return_value=True),
            patch.object(auto_sync, "sync", side_effect=results),
            patch.object(auto_sync, "post_deploy_healthcheck") as mock_health,
        ):
            auto_sync.main()
            mock_health.assert_not_called()
        assert not auto_sync.STATE_FILE.exists()

    def test_never_applies_the_same_sha_twice_across_two_runs(self, auto_sync):
        with (
            patch.object(auto_sync, "remote_sha", return_value="sha-A"),
            patch.object(auto_sync, "ci_passed", return_value=True),
            patch.object(auto_sync, "sync", return_value=_completed(returncode=0)) as mock_sync,
            patch.object(auto_sync, "post_deploy_healthcheck"),
        ):
            auto_sync.main()
            auto_sync.main()
            assert mock_sync.call_count == 2  # só a 1a chamada de main() sincronizou (dry+apply); a 2a não fez nada


class TestPostDeployHealthcheck:
    def test_logs_ok_when_no_import_errors(self, auto_sync):
        with patch.object(auto_sync, "run", return_value=_completed(stdout="No data found")) as mock_run:
            auto_sync.post_deploy_healthcheck()
            mock_run.assert_called_once()

    def test_flags_alert_when_import_errors_present(self, auto_sync, capsys):
        with patch.object(auto_sync, "run", return_value=_completed(stdout="dags/broken.py: SyntaxError")):
            auto_sync.post_deploy_healthcheck()
        log_content = auto_sync.LOG_FILE.read_text()
        assert "ALERTA" in log_content
