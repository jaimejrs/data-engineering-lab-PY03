"""Testes do Modelo 1 — detecção de anomalias em contratos.

Só cobre a parte pura (engenharia de features + treino + score), sem tocar
Trino/rede — `extract_features` é testado só quanto ao formato da query
(a leitura em si depende do stack do lakehouse no ar).
"""

from unittest.mock import patch

import pandas as pd
import pytest

from models import anomaly_detection


def _raw_df(n=30, outlier_valor=None):
    """DataFrame sintético no formato de `extract_features` (mesmas colunas)."""
    data = {
        "id_contrato_origem": [str(i) for i in range(n)],
        "ano": [2026] * n,
        "valor_contrato": [10_000.0 + i * 100 for i in range(n)],
        "flag_emergency": [False] * n,
        "modalidade": ["PREGÃO ELETRÔNICO"] * n,
        "tipo_objeto": ["SERVIÇO"] * n,
        "data_inicio": ["2026-01-01"] * n,
        "data_termino": ["2026-06-01"] * n,
        "historico_credor_infringement": [False] * n,
    }
    if outlier_valor is not None:
        data["valor_contrato"][-1] = outlier_valor
    return pd.DataFrame(data)


class TestBuildFeatureMatrix:
    def test_produces_expected_numeric_columns(self):
        X = anomaly_detection.build_feature_matrix(_raw_df())
        for col in ("valor_contrato", "dias_vigencia", "flag_emergency", "historico_credor_infringement"):
            assert col in X.columns

    def test_one_hot_encodes_modalidade_and_tipo_objeto(self):
        X = anomaly_detection.build_feature_matrix(_raw_df())
        assert "modalidade_PREGÃO ELETRÔNICO" in X.columns
        assert "tipo_objeto_SERVIÇO" in X.columns

    def test_rare_tipo_objeto_grouped_as_outros(self):
        # 9 tipos com 2 contratos cada (mais que TOP_TIPO_OBJETO=8) + 1 tipo raro
        # isolado — só os 8 mais frequentes sobrevivem, o resto vira "OUTROS".
        df = _raw_df(n=19)
        for i in range(9):
            df.loc[df.index[2 * i], "tipo_objeto"] = f"TIPO_{i}"
            df.loc[df.index[2 * i + 1], "tipo_objeto"] = f"TIPO_{i}"
        df.loc[df.index[-1], "tipo_objeto"] = "TIPO_RARO_UNICO"

        X = anomaly_detection.build_feature_matrix(df)
        assert "tipo_objeto_OUTROS" in X.columns
        assert "tipo_objeto_TIPO_RARO_UNICO" not in X.columns

    def test_dias_vigencia_computed_from_dates(self):
        X = anomaly_detection.build_feature_matrix(_raw_df(n=1))
        # 2026-01-01 -> 2026-06-01 = 151 dias
        assert X["dias_vigencia"].iloc[0] == 151

    def test_no_nan_in_output(self):
        df = _raw_df()
        df.loc[df.index[0], "valor_contrato"] = None
        df.loc[df.index[1], "data_inicio"] = None
        X = anomaly_detection.build_feature_matrix(df)
        assert not X.isna().any().any()


class TestScoreAnomalia:
    def test_scores_are_within_unit_range(self):
        X = anomaly_detection.build_feature_matrix(_raw_df(n=40, outlier_valor=5_000_000.0))
        model = anomaly_detection.train_model(X, contamination=0.1)
        scores = anomaly_detection.score_anomalia(model, X)
        assert scores.min() >= 0.0
        assert scores.max() <= 1.0

    def test_obvious_outlier_gets_highest_score(self):
        X = anomaly_detection.build_feature_matrix(_raw_df(n=40, outlier_valor=5_000_000.0))
        model = anomaly_detection.train_model(X, contamination=0.1)
        scores = anomaly_detection.score_anomalia(model, X)
        assert scores.idxmax() == X.index[-1]


class TestFlagAnomalia:
    """flag_anomalia — incorporado do script da Fernanda: classificação
    binária do próprio Isolation Forest, complementar ao score contínuo."""

    def test_returns_boolean_series(self):
        X = anomaly_detection.build_feature_matrix(_raw_df(n=40, outlier_valor=5_000_000.0))
        model = anomaly_detection.train_model(X, contamination=0.1)
        flags = anomaly_detection.flag_anomalia(model, X)
        assert flags.dtype == bool

    def test_obvious_outlier_is_flagged(self):
        X = anomaly_detection.build_feature_matrix(_raw_df(n=40, outlier_valor=5_000_000.0))
        model = anomaly_detection.train_model(X, contamination=0.1)
        flags = anomaly_detection.flag_anomalia(model, X)
        assert flags.loc[X.index[-1]]  # o outlier obviamente inserido no fixture

    def test_flag_rate_is_consistent_with_contamination(self):
        X = anomaly_detection.build_feature_matrix(_raw_df(n=100, outlier_valor=5_000_000.0))
        model = anomaly_detection.train_model(X, contamination=0.1)
        flags = anomaly_detection.flag_anomalia(model, X)
        # contamination=0.1 -> ~10% sinalizado (o próprio scikit-learn usa o
        # score de treino pra fixar o limiar, não é exato, mas fica perto).
        assert 0.05 <= flags.mean() <= 0.15


class TestSaveLoadModel:
    def test_round_trips_model_and_feature_columns(self, tmp_path):
        X = anomaly_detection.build_feature_matrix(_raw_df())
        model = anomaly_detection.train_model(X, contamination=0.1)
        path = tmp_path / "model.joblib"

        anomaly_detection.save_model(model, list(X.columns), path=str(path))
        loaded_model, feature_columns = anomaly_detection.load_model(path=str(path))

        assert feature_columns == list(X.columns)
        pd.testing.assert_series_equal(
            anomaly_detection.score_anomalia(model, X),
            anomaly_detection.score_anomalia(loaded_model, X),
        )


class TestScoreDistribution:
    def test_percentiles_and_threshold_counts(self):
        resultado = pd.DataFrame({"score_anomalia": [0.1, 0.5, 0.72, 0.85, 0.91, 0.96, 1.0]})
        summary = anomaly_detection.summarize_score_distribution(resultado)

        assert summary["n_contratos_score_ge_70"] == 5  # 0.72, 0.85, 0.91, 0.96, 1.0
        assert summary["n_contratos_score_ge_90"] == 3  # 0.91, 0.96, 1.0
        assert summary["n_contratos_score_ge_95"] == 2  # 0.96, 1.0
        assert summary["pct_contratos_score_ge_90"] == pytest.approx(3 / 7)
        assert summary["score_p50"] == resultado["score_anomalia"].median()

    def test_handles_empty_result(self):
        resultado = pd.DataFrame({"score_anomalia": pd.Series(dtype=float)})
        summary = anomaly_detection.summarize_score_distribution(resultado)
        assert summary["pct_contratos_score_ge_70"] == 0.0


class TestWriteScores:
    def test_adds_flag_anomalia_column_via_alter_table_before_replace(self):
        resultado = pd.DataFrame(
            {
                "id_contrato_origem": ["1", "2"],
                "ano": [2026, 2026],
                "score_anomalia": [0.9, 0.1],
                "flag_anomalia": [True, False],
            }
        )
        with (
            patch.object(anomaly_detection.trino_io, "execute") as mock_execute,
            patch.object(anomaly_detection.trino_io, "replace_table") as mock_replace,
        ):
            anomaly_detection.write_scores(resultado)

        alter_sql = mock_execute.call_args[0][0]
        assert "ADD COLUMN IF NOT EXISTS flag_anomalia" in alter_sql
        assert "flag_anomalia" in mock_replace.call_args.kwargs["columns"]
        assert "flag_anomalia" in mock_replace.call_args.kwargs["df"].columns


class TestFeatureQuery:
    def test_reads_from_gold_fato_contrato_and_dimensions(self):
        query = anomaly_detection.FEATURE_QUERY
        assert "iceberg.gold.fato_contrato" in query
        assert "iceberg.gold.dim_credor" in query
        assert "iceberg.gold.dim_modalidade" in query
        assert "iceberg.silver.contratos" in query


class TestRunMlflowTracking:
    """`run()` não deve tocar Trino/MLflow/disco real nos testes — tudo mockado."""

    def _patched_run(self, tmp_path, score_medio_anterior=None, **run_kwargs):
        raw = _raw_df(n=10)
        with (
            patch.object(anomaly_detection, "extract_features", return_value=raw),
            patch.object(anomaly_detection, "write_scores") as mock_write,
            patch.object(anomaly_detection, "configure_mlflow") as mock_configure,
            patch.object(anomaly_detection, "mlflow") as mock_mlflow,
            patch.object(anomaly_detection, "ARTIFACT_PATH", str(tmp_path / "model.joblib")),
            # Sem isso, run() chamaria trino_io.query de verdade (1ª execução
            # simulada por padrão: None, sem execução anterior pra comparar).
            patch.object(anomaly_detection, "_score_medio_execucao_anterior", return_value=score_medio_anterior),
        ):
            resultado = anomaly_detection.run(**run_kwargs)
        return resultado, mock_write, mock_configure, mock_mlflow

    def test_configures_mlflow_experiment_and_starts_run(self, tmp_path):
        _, _, mock_configure, mock_mlflow = self._patched_run(tmp_path)

        mock_configure.assert_called_once_with(anomaly_detection.MLFLOW_EXPERIMENT)
        mock_mlflow.start_run.assert_called_once()

    def test_logs_params_metrics_and_artifact(self, tmp_path):
        resultado, _, _, mock_mlflow = self._patched_run(tmp_path, contamination=0.1)

        params = mock_mlflow.log_params.call_args[0][0]
        assert params["contamination"] == 0.1
        metrics = mock_mlflow.log_metrics.call_args[0][0]
        assert "score_medio" in metrics
        assert "n_contratos_escorados" in metrics
        assert "n_contratos_flag_anomalia" in metrics
        assert "pct_contratos_flag_anomalia" in metrics
        mock_mlflow.set_tag.assert_called_once_with("model_version", anomaly_detection.MODEL_VERSION)
        mock_mlflow.log_artifact.assert_called_once_with(str(tmp_path / "model.joblib"))
        assert "flag_anomalia" in resultado.columns

    def test_logs_model_in_mlflow_native_format(self, tmp_path):
        self._patched_run(tmp_path)
        # mlflow.sklearn é acessado via atributo do mock de `mlflow` — não
        # precisa mockar `mlflow.sklearn` à parte.
        with (
            patch.object(anomaly_detection, "extract_features", return_value=_raw_df(n=10)),
            patch.object(anomaly_detection, "write_scores"),
            patch.object(anomaly_detection, "configure_mlflow"),
            patch.object(anomaly_detection, "mlflow") as mock_mlflow,
            patch.object(anomaly_detection, "ARTIFACT_PATH", str(tmp_path / "model.joblib")),
        ):
            anomaly_detection.run()
        mock_mlflow.sklearn.log_model.assert_called_once()
        assert mock_mlflow.sklearn.log_model.call_args.kwargs.get("artifact_path") == "isolation_forest_model"

    def test_persist_false_still_logs_but_skips_write_scores(self, tmp_path):
        _, mock_write, _, mock_mlflow = self._patched_run(tmp_path, persist=False)

        mock_write.assert_not_called()
        mock_mlflow.log_artifact.assert_called_once()

    def test_gate_alerta_distribuicao_degenerada(self, tmp_path):
        """Gate de qualidade (auditoria de 30/07/2026): p50 == p99 significa
        que o modelo não discriminou NENHUM contrato neste treino."""
        raw = _raw_df(n=10)
        scores_constantes = pd.Series([0.5] * len(raw), name="score_anomalia")
        with (
            patch.object(anomaly_detection, "extract_features", return_value=raw),
            patch.object(anomaly_detection, "score_anomalia", return_value=scores_constantes),
            patch.object(anomaly_detection, "write_scores"),
            patch.object(anomaly_detection, "configure_mlflow"),
            patch.object(anomaly_detection, "mlflow") as mock_mlflow,
            patch.object(anomaly_detection, "ARTIFACT_PATH", str(tmp_path / "model.joblib")),
            patch.object(anomaly_detection, "_score_medio_execucao_anterior", return_value=None),
        ):
            anomaly_detection.run()
        tags = [c.args for c in mock_mlflow.set_tag.call_args_list]
        assert ("alerta_qualidade", "distribuicao_degenerada") in tags

    def test_gate_alerta_drift_score_medio_quando_diferenca_grande(self, tmp_path):
        """Gate de qualidade: mudança grande no score médio vs. a execução
        anterior (consultada de SCORE_TABLE antes do replace_table) dispara
        alerta — sinal de possível regressão a montante."""
        raw = _raw_df(n=10)
        scores_variados = pd.Series([i / 10 for i in range(10)], name="score_anomalia")  # média = 0.45
        with (
            patch.object(anomaly_detection, "extract_features", return_value=raw),
            patch.object(anomaly_detection, "score_anomalia", return_value=scores_variados),
            patch.object(anomaly_detection, "write_scores"),
            patch.object(anomaly_detection, "configure_mlflow"),
            patch.object(anomaly_detection, "mlflow") as mock_mlflow,
            patch.object(anomaly_detection, "ARTIFACT_PATH", str(tmp_path / "model.joblib")),
            patch.object(anomaly_detection, "_score_medio_execucao_anterior", return_value=0.90),
        ):
            anomaly_detection.run()
        tags = [c.args for c in mock_mlflow.set_tag.call_args_list]
        assert ("alerta_qualidade", "drift_score_medio") in tags

    def test_gate_sem_alerta_quando_score_medio_estavel(self, tmp_path):
        raw = _raw_df(n=10)
        scores_variados = pd.Series([i / 10 for i in range(10)], name="score_anomalia")  # média = 0.45
        with (
            patch.object(anomaly_detection, "extract_features", return_value=raw),
            patch.object(anomaly_detection, "score_anomalia", return_value=scores_variados),
            patch.object(anomaly_detection, "write_scores"),
            patch.object(anomaly_detection, "configure_mlflow"),
            patch.object(anomaly_detection, "mlflow") as mock_mlflow,
            patch.object(anomaly_detection, "ARTIFACT_PATH", str(tmp_path / "model.joblib")),
            patch.object(anomaly_detection, "_score_medio_execucao_anterior", return_value=0.40),
        ):
            anomaly_detection.run()
        tags = [c.args for c in mock_mlflow.set_tag.call_args_list]
        assert ("alerta_qualidade", "drift_score_medio") not in tags
        assert ("alerta_qualidade", "distribuicao_degenerada") not in tags


class TestScoreMedioExecucaoAnterior:
    def test_returns_none_when_table_does_not_exist_yet(self):
        with patch.object(anomaly_detection.trino_io, "query", side_effect=Exception("table not found")):
            assert anomaly_detection._score_medio_execucao_anterior() is None

    def test_returns_none_when_result_is_empty_or_null(self):
        with patch.object(anomaly_detection.trino_io, "query", return_value=pd.DataFrame({"media": [None]})):
            assert anomaly_detection._score_medio_execucao_anterior() is None

    def test_returns_the_average_when_available(self):
        with patch.object(anomaly_detection.trino_io, "query", return_value=pd.DataFrame({"media": [0.1647]})):
            assert anomaly_detection._score_medio_execucao_anterior() == pytest.approx(0.1647)
