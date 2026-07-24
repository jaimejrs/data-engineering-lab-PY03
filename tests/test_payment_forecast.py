"""Testes do Modelo 2 — previsão trimestral de pagamentos por órgão.

Só cobre a parte pura (painel + features + treino + avaliação + previsão), sem
tocar Trino/rede — as queries (`PAGAMENTO_QUERY`/`CONTRATOS_VIGENCIA_QUERY`) são
testadas só quanto ao formato.
"""

import pandas as pd

from models import payment_forecast as pf


def _pagamentos_df(orgaos=("A",), anos=(2024, 2025), trimestres=(1, 2, 3, 4), valor_base=10_000.0):
    """8 trimestres por órgão (2 anos), valor crescente por trimestre — série limpa
    o bastante para o lag/target não ficarem todos NaN nos testes."""
    rows = []
    t = 0
    for orgao in orgaos:
        for ano in anos:
            for trimestre in trimestres:
                rows.append(
                    {
                        "sk_orgao": orgao,
                        "codigo_orgao": orgao,
                        "nome_orgao": f"ÓRGÃO {orgao}",
                        "valor": valor_base + t * 500,
                        "modalidade": "3.3.90.18",  # natureza orçamentária (proxy de tipo de despesa)
                        "ano": ano,
                        "trimestre": trimestre,
                    }
                )
                t += 1
    return pd.DataFrame(rows)


def _contratos_df(orgaos=("A",)):
    return pd.DataFrame(
        {
            "sk_orgao": list(orgaos),
            "valor_contrato": [100_000.0] * len(orgaos),
            "data_inicio": ["2024-01-01"] * len(orgaos),
            "data_termino": ["2025-12-31"] * len(orgaos),
        }
    )


class TestBuildQuarterlyPanel:
    def test_aggregates_one_row_per_org_quarter(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(), _contratos_df())
        assert len(panel) == 8  # 2 anos x 4 trimestres, 1 órgão
        assert set(panel.columns) >= {
            "valor_trimestre", "lag_1_trimestre", "lag_4_trimestres", "target_proximo_trimestre",
            "valor_contratado_ativo", "flag_ano_eleitoral",
        }

    def test_lag_1_matches_previous_quarter_value(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(), _contratos_df()).sort_values(["ano", "trimestre"])
        segunda_linha = panel.iloc[1]
        primeira_linha = panel.iloc[0]
        assert segunda_linha["lag_1_trimestre"] == primeira_linha["valor_trimestre"]

    def test_target_is_next_quarter_value(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(), _contratos_df()).sort_values(["ano", "trimestre"])
        primeira_linha = panel.iloc[0]
        segunda_linha = panel.iloc[1]
        assert primeira_linha["target_proximo_trimestre"] == segunda_linha["valor_trimestre"]

    def test_last_quarter_has_no_target(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(), _contratos_df()).sort_values(["ano", "trimestre"])
        assert pd.isna(panel.iloc[-1]["target_proximo_trimestre"])

    def test_valor_contratado_ativo_within_vigencia(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(), _contratos_df())
        # Contrato vigente 2024-01-01 a 2025-12-31 cobre todos os 8 trimestres do fixture.
        assert (panel["valor_contratado_ativo"] == 100_000.0).all()

    def test_flag_ano_eleitoral(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(anos=(2024, 2025)), _contratos_df())
        assert panel.loc[panel["ano"] == 2024, "flag_ano_eleitoral"].eq(1).all()
        assert panel.loc[panel["ano"] == 2025, "flag_ano_eleitoral"].eq(0).all()


class TestBuildFeatureMatrix:
    def test_drops_rows_without_lag_1(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(), _contratos_df())
        X = pf.build_feature_matrix(panel)
        # A primeira linha (sem trimestre anterior) não entra na matriz de features.
        assert len(X) == len(panel) - 1

    def test_no_nan_in_output(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(), _contratos_df())
        X = pf.build_feature_matrix(panel)
        assert not X.isna().any().any()


class TestTrainAndPredict:
    def test_quantile_predictions_are_monotonic(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=("A", "B")), _contratos_df(orgaos=("A", "B")))
        X = pf.build_feature_matrix(panel)
        y = panel.loc[X.index, "target_proximo_trimestre"]
        train_idx = X.index[y.notna()]

        models = pf.train_models(X.loc[train_idx], y.loc[train_idx])
        preds = pf.predict_quantiles(models, X.loc[train_idx])

        assert (preds["p10"] <= preds["p50"]).all()
        assert (preds["p50"] <= preds["p90"]).all()

    def test_forecast_next_quarter_targets_last_known_row_per_org(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=("A", "B")), _contratos_df(orgaos=("A", "B")))
        X = pf.build_feature_matrix(panel)
        y = panel.loc[X.index, "target_proximo_trimestre"]
        train_idx = X.index[y.notna()]

        models = pf.train_models(X.loc[train_idx], y.loc[train_idx])
        resultado = pf.forecast_next_quarter(panel, X, models)

        assert len(resultado) == 2  # 1 previsão por órgão (o último trimestre conhecido)
        assert set(resultado["sk_orgao"]) == {"A", "B"}
        assert (resultado["valor_previsto_p10"] <= resultado["valor_previsto_p50"]).all()
        assert (resultado["valor_previsto_p50"] <= resultado["valor_previsto_p90"]).all()

    def test_evaluate_returns_holdout_metrics(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=("A", "B")), _contratos_df(orgaos=("A", "B")))
        X = pf.build_feature_matrix(panel)
        metrics = pf.evaluate(panel, X)
        assert "mae_mediana" in metrics
        assert "cobertura_intervalo_80pct" in metrics
        assert metrics["mae_mediana"] >= 0


class TestSaveLoadModel:
    def test_round_trips_models_and_feature_columns(self, tmp_path):
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=("A", "B")), _contratos_df(orgaos=("A", "B")))
        X = pf.build_feature_matrix(panel)
        y = panel.loc[X.index, "target_proximo_trimestre"]
        train_idx = X.index[y.notna()]
        models = pf.train_models(X.loc[train_idx], y.loc[train_idx])
        path = tmp_path / "model.joblib"

        pf.save_model(models, list(X.columns), path=str(path))
        loaded_models, feature_columns = pf.load_model(path=str(path))

        assert feature_columns == list(X.columns)
        assert set(loaded_models) == set(models)


class TestQueries:
    def test_pagamento_query_reads_from_fato_ordem_bancaria(self):
        assert "iceberg.gold.fato_ordem_bancaria" in pf.PAGAMENTO_QUERY
        assert "iceberg.gold.dim_tempo" in pf.PAGAMENTO_QUERY
        assert "iceberg.gold.dim_orgao" in pf.PAGAMENTO_QUERY

    def test_pagamento_query_excludes_cancelled_ob(self):
        assert "flag_cancelada" in pf.PAGAMENTO_QUERY

    def test_contratos_query_reads_vigencia_from_silver(self):
        assert "iceberg.gold.fato_contrato" in pf.CONTRATOS_VIGENCIA_QUERY
        assert "iceberg.silver.contratos" in pf.CONTRATOS_VIGENCIA_QUERY
