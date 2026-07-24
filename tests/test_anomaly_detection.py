"""Testes do Modelo 1 — detecção de anomalias em contratos.

Só cobre a parte pura (engenharia de features + treino + score), sem tocar
Trino/rede — `extract_features` é testado só quanto ao formato da query
(a leitura em si depende do stack do lakehouse no ar).
"""

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


class TestFeatureQuery:
    def test_reads_from_gold_fato_contrato_and_dimensions(self):
        query = anomaly_detection.FEATURE_QUERY
        assert "iceberg.gold.fato_contrato" in query
        assert "iceberg.gold.dim_credor" in query
        assert "iceberg.gold.dim_modalidade" in query
        assert "iceberg.silver.contratos" in query
