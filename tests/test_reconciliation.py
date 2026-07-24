"""Testes de src/reconciliation.py — item 6 de docs/06-analise-critica.md.

Mocka `src.trino_io.execute` inteiro; não toca rede/Trino.
"""

from unittest.mock import call, patch

from src import reconciliation


class TestRecordBronzeCounts:
    def test_ensures_table_exists_before_writing(self):
        with patch.object(reconciliation.trino_io, "execute") as mock_execute:
            reconciliation.record_bronze_counts("2026-07-24", {"empenhos": 100})

        assert mock_execute.call_args_list[0] == call(reconciliation.DDL)

    def test_deletes_before_inserting_per_source(self):
        with patch.object(reconciliation.trino_io, "execute") as mock_execute:
            reconciliation.record_bronze_counts("2026-07-24", {"empenhos": 100, "contratos": 50})

        calls = [c.args[0] for c in mock_execute.call_args_list]
        assert any("DELETE FROM" in c and "'empenhos'" in c and "'2026-07-24'" in c for c in calls)
        assert any("DELETE FROM" in c and "'contratos'" in c for c in calls)

    def test_inserts_the_exact_record_count(self):
        with patch.object(reconciliation.trino_io, "execute") as mock_execute:
            reconciliation.record_bronze_counts("2026-07-24", {"empenhos": 1376379})

        calls = [c.args[0] for c in mock_execute.call_args_list]
        assert any("INSERT INTO" in c and "1376379" in c for c in calls)

    def test_writes_one_delete_and_insert_pair_per_source(self):
        with patch.object(reconciliation.trino_io, "execute") as mock_execute:
            reconciliation.record_bronze_counts(
                "2026-07-24", {"empenhos": 1, "ordem_bancaria_orcamentaria": 2, "contratos": 3, "unidade_gestora": 4}
            )

        # 1 DDL + (1 DELETE + 1 INSERT) por fonte = 1 + 4*2 = 9
        assert mock_execute.call_count == 9

    def test_handles_empty_counts_without_error(self):
        with patch.object(reconciliation.trino_io, "execute") as mock_execute:
            reconciliation.record_bronze_counts("2026-07-24", {})

        assert mock_execute.call_count == 1  # só o DDL
