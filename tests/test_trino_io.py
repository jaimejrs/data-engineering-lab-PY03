"""Testes de `src/trino_io.py` — `bulk_insert`/`replace_table` com uma conexão
fake (sem depender de um Trino real). Item 15 de docs/06-analise-critica.md:
`replace_table` ganhou staging + swap atômico (`ALTER TABLE ... RENAME TO`)
no lugar de `DELETE FROM` + `INSERT` direto na tabela final.
"""

import pandas as pd

from src import trino_io


class FakeCursor:
    def __init__(self, log: list[str]):
        self._log = log

    def execute(self, sql: str):
        self._log.append(sql)

    def fetchall(self):
        return []


class FakeConnection:
    def __init__(self):
        self.log: list[str] = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self.log)

    def close(self):
        self.closed = True


def _payload():
    return pd.DataFrame({"id_contrato_origem": ["1", "2"], "score_anomalia": [0.1, 0.9]})


DDL = """
    CREATE TABLE IF NOT EXISTS iceberg.gold.score_anomalia_contrato (
        id_contrato_origem varchar,
        score_anomalia double
    )
"""


class TestBulkInsert:
    def test_chunks_rows_and_drains_cursor(self):
        conn = FakeConnection()
        df = pd.DataFrame({"a": range(7), "b": range(7)})
        trino_io.bulk_insert("t", df, ["a", "b"], conn=conn, chunk_size=3)
        inserts = [s for s in conn.log if s.startswith("INSERT INTO")]
        assert len(inserts) == 3  # 7 linhas em lotes de 3 -> 3, 3, 1
        assert not conn.closed  # conexão passada de fora não é fechada pela função

    def test_empty_dataframe_does_not_execute_anything(self):
        conn = FakeConnection()
        trino_io.bulk_insert("t", pd.DataFrame(), ["a"], conn=conn)
        assert conn.log == []

    def test_owns_and_closes_connection_when_none_passed(self, monkeypatch):
        fake = FakeConnection()
        monkeypatch.setattr(trino_io, "connect", lambda schema="gold": fake)
        trino_io.bulk_insert("t", pd.DataFrame({"a": [1]}), ["a"])
        assert fake.closed


class TestReplaceTable:
    def test_never_deletes_from_final_table(self):
        """A versão antiga (`DELETE FROM` + `INSERT` direto) apagava a tabela
        final antes de reescrevê-la. A versão nova nunca deve fazer isso —
        toda a gravação acontece numa tabela de staging à parte."""
        conn = FakeConnection()
        trino_io.replace_table(
            table="iceberg.gold.score_anomalia_contrato",
            df=_payload(),
            columns=["id_contrato_origem", "score_anomalia"],
            ddl=DDL,
            conn=conn,
        )
        assert not any("DELETE FROM iceberg.gold.score_anomalia_contrato" in s for s in conn.log)

    def test_writes_batch_into_staging_table_not_final(self):
        conn = FakeConnection()
        trino_io.replace_table(
            table="iceberg.gold.score_anomalia_contrato",
            df=_payload(),
            columns=["id_contrato_origem", "score_anomalia"],
            ddl=DDL,
            conn=conn,
        )
        inserts = [s for s in conn.log if s.startswith("INSERT INTO")]
        assert len(inserts) == 1
        assert "iceberg.gold.score_anomalia_contrato__staging (" in inserts[0]
        assert "iceberg.gold.score_anomalia_contrato__staging__dedup" not in inserts[0]

    def test_dedup_via_select_distinct_from_staging(self):
        conn = FakeConnection()
        trino_io.replace_table(
            table="iceberg.gold.score_anomalia_contrato",
            df=_payload(),
            columns=["id_contrato_origem", "score_anomalia"],
            ddl=DDL,
            conn=conn,
        )
        dedup_stmts = [s for s in conn.log if "SELECT DISTINCT" in s]
        assert len(dedup_stmts) == 1
        assert "iceberg.gold.score_anomalia_contrato__staging__dedup" in dedup_stmts[0]
        assert "FROM iceberg.gold.score_anomalia_contrato__staging" in dedup_stmts[0]

    def test_swap_order_final_table_never_missing(self):
        """A ordem tem que garantir que `table` só é renomeada pra `__old`
        DEPOIS que a versão nova (`__staging__dedup`) já está pronta e
        completa — nunca deixa `table` inexistente por mais que o instante de
        um único ALTER TABLE (operação de metadado, não de dado)."""
        conn = FakeConnection()
        trino_io.replace_table(
            table="iceberg.gold.score_anomalia_contrato",
            df=_payload(),
            columns=["id_contrato_origem", "score_anomalia"],
            ddl=DDL,
            conn=conn,
        )
        renames = [s for s in conn.log if "RENAME TO" in s]
        assert len(renames) == 2
        assert "ALTER TABLE iceberg.gold.score_anomalia_contrato RENAME TO" in renames[0]
        assert renames[0].strip().endswith("__old")
        assert "__staging__dedup RENAME TO iceberg.gold.score_anomalia_contrato" in renames[1]

        dedup_idx = next(i for i, s in enumerate(conn.log) if "SELECT DISTINCT" in s)
        first_rename_idx = conn.log.index(renames[0])
        assert dedup_idx < first_rename_idx  # staging__dedup já existe antes de tocar na tabela final

    def test_cleans_up_leftovers_from_a_previous_crashed_run(self):
        """Se uma execução anterior morreu no meio (ex: escritor fantasma via
        `docker exec`, item 15), pode sobrar `__staging`/`__staging__dedup`/
        `__old`. A próxima chamada tem que limpar antes de recomeçar."""
        conn = FakeConnection()
        trino_io.replace_table(
            table="iceberg.gold.score_anomalia_contrato",
            df=_payload(),
            columns=["id_contrato_origem", "score_anomalia"],
            ddl=DDL,
            conn=conn,
        )
        drops = [s for s in conn.log if s.startswith("DROP TABLE IF EXISTS")]
        dropped_tables = {s.replace("DROP TABLE IF EXISTS ", "").strip() for s in drops}
        assert dropped_tables == {
            "iceberg.gold.score_anomalia_contrato__staging",
            "iceberg.gold.score_anomalia_contrato__staging__dedup",
            "iceberg.gold.score_anomalia_contrato__old",
        }

    def test_final_table_ends_with_no_staging_artifacts_left_behind(self):
        conn = FakeConnection()
        trino_io.replace_table(
            table="iceberg.gold.score_anomalia_contrato",
            df=_payload(),
            columns=["id_contrato_origem", "score_anomalia"],
            ddl=DDL,
            conn=conn,
        )
        # última operação sobre cada tabela auxiliar tem que ser um DROP
        last_old_op = [s for s in conn.log if "__old" in s][-1]
        last_staging_op = [s for s in conn.log if s.count("__staging") == 1 and "__dedup" not in s][-1]
        assert last_old_op.startswith("DROP TABLE IF EXISTS")
        assert last_staging_op.startswith("DROP TABLE IF EXISTS")

    def test_staging_ddl_uses_staging_table_name(self):
        conn = FakeConnection()
        trino_io.replace_table(
            table="iceberg.gold.score_anomalia_contrato",
            df=_payload(),
            columns=["id_contrato_origem", "score_anomalia"],
            ddl=DDL,
            conn=conn,
        )
        creates = [s for s in conn.log if "CREATE TABLE IF NOT EXISTS" in s and "score_anomalia double" in s]
        assert len(creates) == 2  # a `ddl` original (garante `table`) + a versão staging
        assert any("iceberg.gold.score_anomalia_contrato__staging (" in s for s in creates)
        assert any("iceberg.gold.score_anomalia_contrato (" in s and "__staging" not in s for s in creates)
