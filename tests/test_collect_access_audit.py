"""Testes de `deploy/server-lakehouse/collect_access_audit.py` — auditoria de
acesso (sessão SSH via journalctl, comando executado via auditd/ausearch).
Carregado via `importlib` (script standalone rodado por cron, igual
collect_infra_metrics.py). `run()` é sempre mockado — nenhum teste aqui toca
journalctl/auditd/Trino real.
"""

import importlib.util
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "deploy" / "server-lakehouse" / "collect_access_audit.py"


@pytest.fixture
def mod(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("collect_access_audit", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "STATE_FILE", tmp_path / ".access-audit-state.json")
    return module


def _completed(stdout="", returncode=0):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr="")


JOURNAL_SAMPLE = (
    "2026-07-26T13:55:51+00:00 datalab-server sshd-session[1188185]: "
    "Accepted publickey for dataadm from 100.101.236.119 port 54143 ssh2: ED25519 SHA256:abc\n"
    "2026-07-26T13:55:51+00:00 datalab-server sshd-session[1188185]: "
    "pam_unix(sshd:session): session opened for user dataadm(uid=1000) by dataadm(uid=0)\n"
    "2026-07-26T13:56:02+00:00 datalab-server sshd-session[1188185]: "
    "pam_unix(sshd:session): session closed for user dataadm\n"
)

AUSEARCH_SAMPLE = (
    "type=PROCTITLE msg=audit(07/26/26 13:55:13.347:651) : proctitle=docker ps -a \n"
    "type=PATH msg=audit(07/26/26 13:55:13.347:651) : item=0 name=/usr/bin/docker\n"
    "type=CWD msg=audit(07/26/26 13:55:13.347:651) : cwd=/home/dataadm\n"
    "type=EXECVE msg=audit(07/26/26 13:55:13.347:651) : argc=2 a0=docker a1=ps \n"
    "type=SYSCALL msg=audit(07/26/26 13:55:13.347:651) : arch=x86_64 syscall=execve success=yes exit=0 "
    "a0=0x1 a1=0x2 a2=0x3 a3=0x4 items=2 ppid=100 pid=101 auid=dataadm uid=dataadm gid=dataadm "
    "euid=dataadm suid=dataadm fsuid=dataadm egid=dataadm sgid=dataadm fsgid=dataadm tty=pts0 ses=7050 "
    "comm=docker exe=/usr/bin/docker subj=unconfined key=cmd_exec \n"
    "----\n"
    "type=PROCTITLE msg=audit(07/26/26 13:55:14.000:652) : proctitle=/usr/lib/trino/bin/health-check \n"
    "type=SYSCALL msg=audit(07/26/26 13:55:14.000:652) : arch=x86_64 syscall=execve success=yes exit=0 "
    "a0=0x1 a1=0x2 a2=0x3 a3=0x4 items=2 ppid=1 pid=2 auid=unset uid=trino gid=trino "
    "euid=trino suid=trino fsuid=trino egid=trino sgid=trino fsgid=trino tty=(none) ses=4294967295 "
    "comm=health-check exe=/usr/lib/trino/bin/health-check subj=unconfined key=cmd_exec \n"
    "----\n"
    "type=PROCTITLE msg=audit(07/26/26 13:56:00.500:660) : proctitle=sudo -S apt-get update \n"
    "type=SYSCALL msg=audit(07/26/26 13:56:00.500:660) : arch=x86_64 syscall=execve success=yes exit=0 "
    "a0=0x1 a1=0x2 a2=0x3 a3=0x4 items=2 ppid=200 pid=201 auid=dataadm uid=dataadm gid=dataadm "
    "euid=root suid=root fsuid=root egid=dataadm sgid=dataadm fsgid=dataadm tty=pts0 ses=7050 "
    "comm=sudo exe=/usr/bin/sudo subj=unconfined key=cmd_exec \n"
)


class TestCollectSshSessions:
    def test_parses_login_and_logout_events(self, mod):
        with patch.object(mod, "run", return_value=_completed(JOURNAL_SAMPLE)):
            rows = mod.collect_ssh_sessions(None)
        assert len(rows) == 2
        login, logout = rows
        assert login == {
            "evento": "login",
            "usuario": "dataadm",
            "origem_ip": "100.101.236.119",
            "sessao_pid": "1188185",
            "timestamp_evento": "2026-07-26 13:55:51",
        }
        assert logout["evento"] == "logout"
        assert logout["sessao_pid"] == "1188185"
        assert logout["origem_ip"] is None

    def test_empty_journal_returns_empty_list(self, mod):
        with patch.object(mod, "run", return_value=_completed("")):
            assert mod.collect_ssh_sessions(None) == []


class TestCollectCommandExecutions:
    def test_captures_interactive_commands_with_proctitle(self, mod):
        with patch.object(mod, "run", return_value=_completed(AUSEARCH_SAMPLE)):
            rows = mod.collect_command_executions()
        comandos = [r["comando"] for r in rows]
        assert "docker ps -a" in comandos
        assert "sudo -S apt-get update" in comandos

    def test_excludes_events_with_unset_auid(self, mod):
        # health-check (auid=unset) é processo interno de container/systemd,
        # nunca passou por login — não deve aparecer.
        with patch.object(mod, "run", return_value=_completed(AUSEARCH_SAMPLE)):
            rows = mod.collect_command_executions()
        executaveis = [r["executavel"] for r in rows]
        assert "/usr/lib/trino/bin/health-check" not in executaveis

    def test_captures_effective_user_when_sudo(self, mod):
        with patch.object(mod, "run", return_value=_completed(AUSEARCH_SAMPLE)):
            rows = mod.collect_command_executions()
        sudo_row = next(r for r in rows if "sudo" in r["comando"])
        assert sudo_row["usuario"] == "dataadm"
        assert sudo_row["executado_como"] == "root"

    def test_parses_timestamp_to_iso(self, mod):
        with patch.object(mod, "run", return_value=_completed(AUSEARCH_SAMPLE)):
            rows = mod.collect_command_executions()
        docker_row = next(r for r in rows if r["comando"] == "docker ps -a")
        assert docker_row["timestamp_evento"] == "2026-07-26 13:55:13.347"

    def test_no_matches_returns_empty_list(self, mod):
        with patch.object(mod, "run", return_value=_completed("<no matches>", returncode=1)):
            assert mod.collect_command_executions() == []

    def test_real_failure_raises_instead_of_silently_returning_empty(self, mod):
        # Achado real em produção: sudo negado silenciosamente virava lista
        # vazia, e o estado era salvo como "processado" — perdia o intervalo
        # pra sempre. Falha real agora precisa propagar, não ser engolida.
        with patch.object(mod, "run", return_value=_completed("sudo: a password is required", returncode=1)):
            with pytest.raises(RuntimeError):
                mod.collect_command_executions()


class TestBuildInsertSql:
    def test_build_insert_sessoes_escapes_quotes(self, mod):
        rows = [
            {
                "evento": "login",
                "usuario": "o'brien",
                "origem_ip": "1.2.3.4",
                "sessao_pid": "1",
                "timestamp_evento": "2026-07-26 10:00:00",
            }
        ]
        sql = mod.build_insert_sessoes(rows, "2026-07-26 10:05:00")
        assert "o''brien" in sql
        assert "sessoes_ssh" in sql

    def test_build_insert_comandos_handles_null_fields(self, mod):
        rows = [
            {
                "usuario": "dataadm",
                "executado_como": "dataadm",
                "comando": "ls -la",
                "executavel": "/bin/ls",
                "sessao_id": "7050",
                "tty": "pts0",
                "sucesso": True,
                "timestamp_evento": "2026-07-26 10:00:00.123",
            }
        ]
        sql = mod.build_insert_comandos(rows, "2026-07-26 10:05:00")
        assert "comandos_executados" in sql
        assert "true" in sql


class TestInsertInChunks:
    def test_splits_rows_into_multiple_trino_calls(self, mod, monkeypatch):
        monkeypatch.setattr(mod, "CHUNK_SIZE", 2)
        calls = []
        with patch.object(mod, "trino_execute", side_effect=lambda sql, c: calls.append(sql)):
            rows = [
                {
                    "evento": "login",
                    "usuario": f"u{i}",
                    "origem_ip": "1.2.3.4",
                    "sessao_pid": str(i),
                    "timestamp_evento": "2026-07-26 10:00:00",
                }
                for i in range(5)
            ]
            mod._insert_in_chunks(rows, mod.build_insert_sessoes, "2026-07-26 10:05:00", "fake")
        assert len(calls) == 3  # 2 + 2 + 1


class TestMain:
    def test_main_updates_state_file_after_run(self, mod):
        calls = []

        def fake_run(args):
            calls.append(args)
            if args[0] == "journalctl":
                return _completed(JOURNAL_SAMPLE)
            if any("ausearch" in a for a in args):
                return _completed(AUSEARCH_SAMPLE)
            return _completed("")

        with patch.object(mod, "run", side_effect=fake_run):
            mod.main("fake_trino_container")

        assert mod.STATE_FILE.exists()
        import json

        state = json.loads(mod.STATE_FILE.read_text())
        assert "ultimo_journal_iso" in state
        assert "ultimo_audit_mmddyy" in state
        assert "ultimo_audit_hhmmss" in state

    def test_main_creates_tables_before_inserting(self, mod):
        calls = []

        def fake_run(args):
            calls.append(args)
            return _completed("")

        with patch.object(mod, "run", side_effect=fake_run):
            mod.main("fake_trino_container")

        exec_calls = [c for c in calls if c[:2] == ["docker", "exec"]]
        assert any("CREATE TABLE" in c[-1] and "sessoes_ssh" in c[-1] for c in exec_calls)
        assert any("CREATE TABLE" in c[-1] and "comandos_executados" in c[-1] for c in exec_calls)
