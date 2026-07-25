"""Testes do componente de IA Generativa — relatório narrativo (tarefa 25).

Nenhum teste chama a API real da OpenAI nem o Trino real: o cliente OpenAI é
injetado (fake) em `generate_narrative`, e a leitura/gravação via Trino é
mockada, igual ao padrão dos testes dos outros dois modelos.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

from models import narrative_report as nr


def _anomalias_df(n=2):
    return pd.DataFrame(
        {
            "id_contrato_origem": [str(i) for i in range(n)],
            "ano": [2026] * n,
            "score_anomalia": [0.95, 0.80][:n],
            "valor_contrato": [5_000_000.0, 1_200_000.0][:n],
            "orgao": ["Secretaria da Fazenda", "Secretaria da Saúde"][:n],
            "credor": ["Fornecedor X LTDA", "Fornecedor Y LTDA"][:n],
            "modalidade": ["DISPENSA", "PREGÃO ELETRÔNICO"][:n],
            "flag_emergency": [True, False][:n],
        }
    )


def _previsoes_df(n=2):
    return pd.DataFrame(
        {
            "codigo_orgao": ["22000000", "24000000"][:n],
            "nome_orgao": ["Secretaria da Fazenda", "Secretaria da Saúde"][:n],
            "ano_previsto": [2026] * n,
            "trimestre_previsto": [4, 4][:n],
            "valor_previsto_p10": [900_000.0, 400_000.0][:n],
            "valor_previsto_p50": [1_000_000.0, 500_000.0][:n],
            "valor_previsto_p90": [1_100_000.0, 600_000.0][:n],
        }
    )


def _fake_openai_client(reply_text="# Relatório\nConteúdo gerado."):
    client = MagicMock()
    response = MagicMock()
    response.choices = [MagicMock(message=MagicMock(content=reply_text))]
    client.chat.completions.create.return_value = response
    return client


class TestBuildPrompt:
    def test_includes_formatted_reais_and_percentages(self):
        _, user_prompt = nr.build_prompt(_anomalias_df(), _previsoes_df())
        assert "R$ 5.000.000,00" in user_prompt
        assert "95%" in user_prompt

    def test_system_prompt_forbids_technical_jargon(self):
        system_prompt, _ = nr.build_prompt(_anomalias_df(), _previsoes_df())
        assert "score" in system_prompt  # citado como termo a EVITAR
        assert "gestores públicos" in system_prompt

    def test_handles_empty_anomalias_and_previsoes(self):
        _, user_prompt = nr.build_prompt(pd.DataFrame(), pd.DataFrame())
        assert "Nenhum contrato" in user_prompt
        assert "Nenhuma previsão" in user_prompt

    def test_flags_emergency_contracts(self):
        _, user_prompt = nr.build_prompt(_anomalias_df(), _previsoes_df())
        assert "contratação emergencial" in user_prompt


class TestGenerateNarrative:
    def test_calls_openai_with_configured_model_and_returns_content(self):
        client = _fake_openai_client("# Relatório\nTexto de teste.")
        result = nr.generate_narrative(_anomalias_df(), _previsoes_df(), client=client, model="gpt-4o-mini")

        assert result == "# Relatório\nTexto de teste."
        _, kwargs = client.chat.completions.create.call_args
        assert kwargs["model"] == "gpt-4o-mini"
        assert kwargs["messages"][0]["role"] == "system"
        assert kwargs["messages"][1]["role"] == "user"

    def test_does_not_touch_network_when_client_injected(self):
        # Garante que nenhum client real é criado quando um fake é passado —
        # se `OpenAI()` fosse instanciado de verdade sem API key, levantaria erro.
        client = _fake_openai_client()
        with patch("models.narrative_report.OpenAI") as mock_openai_cls:
            nr.generate_narrative(_anomalias_df(), _previsoes_df(), client=client)
            mock_openai_cls.assert_not_called()


class TestSaveReportFile:
    def test_writes_markdown_file_with_timestamped_name(self, tmp_path):
        gerado_em = datetime(2026, 7, 24, 12, 0, 0, tzinfo=timezone.utc)
        path = nr.save_report_file("# Relatório\nConteúdo.", gerado_em, report_dir=str(tmp_path))

        assert path.endswith("relatorio_20260724_120000.md")
        with open(path, encoding="utf-8") as f:
            assert f.read() == "# Relatório\nConteúdo."


class TestWriteReportMetadata:
    def test_writes_single_row_with_expected_columns(self):
        gerado_em = datetime(2026, 7, 24, 12, 0, 0, tzinfo=timezone.utc)
        with patch.object(nr.trino_io, "replace_table") as mock_replace:
            nr.write_report_metadata(
                conteudo="# Relatório",
                gerado_em=gerado_em,
                num_anomalias=2,
                num_previsoes=3,
                llm_model="gpt-4o-mini",
            )

        assert mock_replace.call_count == 1
        _, kwargs = mock_replace.call_args
        assert kwargs["table"] == nr.REPORT_TABLE
        df = kwargs["df"]
        assert len(df) == 1
        assert df.iloc[0]["num_contratos_anomalos"] == 2
        assert df.iloc[0]["num_orgaos_previstos"] == 3
        assert df.iloc[0]["llm_model"] == "gpt-4o-mini"
        assert df.iloc[0]["conteudo_markdown"] == "# Relatório"


class TestQueries:
    def test_top_anomalias_query_reads_score_and_fato_contrato(self):
        assert "iceberg.gold.score_anomalia_contrato" in nr.TOP_ANOMALIAS_QUERY
        assert "iceberg.gold.fato_contrato" in nr.TOP_ANOMALIAS_QUERY

    def test_top_previsoes_query_reads_forecast_table(self):
        assert "iceberg.gold.previsao_pagamento_orgao" in nr.TOP_PREVISOES_QUERY


class TestRun:
    def test_run_orchestrates_extract_generate_save_persist(self, tmp_path):
        with (
            patch.object(nr, "extract_top_anomalias", return_value=_anomalias_df()) as mock_anomalias,
            patch.object(nr, "extract_top_previsoes", return_value=_previsoes_df()) as mock_previsoes,
            patch.object(nr, "generate_narrative", return_value="# Relatório\nOK.") as mock_generate,
            patch.object(nr, "REPORT_DIR", str(tmp_path)),
            patch.object(nr, "write_report_metadata") as mock_write,
        ):
            resultado = nr.run(top_anomalias=5, top_previsoes=5)

        mock_anomalias.assert_called_once_with(5)
        mock_previsoes.assert_called_once_with(5)
        mock_generate.assert_called_once()
        mock_write.assert_called_once()
        assert resultado == "# Relatório\nOK."

    def test_run_skips_persist_when_disabled(self, tmp_path):
        with (
            patch.object(nr, "extract_top_anomalias", return_value=_anomalias_df()),
            patch.object(nr, "extract_top_previsoes", return_value=_previsoes_df()),
            patch.object(nr, "generate_narrative", return_value="# Relatório"),
            patch.object(nr, "REPORT_DIR", str(tmp_path)),
            patch.object(nr, "write_report_metadata") as mock_write,
        ):
            nr.run(persist=False)

        mock_write.assert_not_called()
