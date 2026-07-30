"""Testes do Modelo 2 — previsão trimestral de pagamentos por órgão.

Só cobre a parte pura (painel + features + treino + avaliação + previsão), sem
tocar Trino/rede — as queries (`PAGAMENTO_QUERY`/`CONTRATOS_VIGENCIA_QUERY`) são
testadas só quanto ao formato.
"""

from unittest.mock import patch

import pandas as pd

from models import payment_forecast as pf


def _pagamentos_df(orgaos=("A",), anos=(2024, 2025), trimestres=(1, 2, 3, 4), valor_base=10_000.0):
    """8 trimestres por órgão (2 anos), valor crescente por trimestre — série limpa
    o bastante para o lag/target não ficarem todos NaN nos testes.

    Sem `sk_orgao` de propósito — o pipeline usa só `codigo_orgao` (estável
    entre anos) desde a correção do bug de agrupamento por `sk_orgao`
    versionado por ano (`dim_orgao.sk_orgao = md5(codigo, ano)`, muda todo
    ano pro MESMO órgão físico — ver topo de `models/payment_forecast.py`).
    """
    rows = []
    t = 0
    for orgao in orgaos:
        for ano in anos:
            for trimestre in trimestres:
                rows.append(
                    {
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
            "codigo_orgao": list(orgaos),
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
            "valor_trimestre",
            "lag_1_trimestre",
            "lag_4_trimestres",
            "target_proximo_trimestre",
            "valor_contratado_ativo",
            "flag_ano_eleitoral",
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

    def test_flag_ano_eleitoral_reflects_target_quarter_year_not_current_row_year(self):
        """Prova a correção do bug (30/07/2026, auditoria de rigor científico):
        a flag precisa refletir o ano do trimestre PREVISTO, não da linha atual.
        Em trimestre=4 o alvo é T1 do ano seguinte — um ano diferente, de
        paridade eleitoral possivelmente invertida. Antes da correção, essa
        transição (25% das linhas) carregava o valor errado."""
        panel = pf.build_quarterly_panel(_pagamentos_df(anos=(2024, 2025)), _contratos_df())

        # 2024 (par): T1-T3 ainda preveem dentro de 2024 (par) -> flag=1.
        linhas_2024 = panel[(panel["ano"] == 2024) & (panel["trimestre"] < 4)]
        assert linhas_2024["flag_ano_eleitoral"].eq(1).all()
        # 2024-T4 prevê 2025-T1 (ímpar) -> flag=0, não 1.
        linha_2024_t4 = panel[(panel["ano"] == 2024) & (panel["trimestre"] == 4)]
        assert linha_2024_t4["flag_ano_eleitoral"].eq(0).all()

        # 2025 (ímpar): T1-T3 ainda preveem dentro de 2025 (ímpar) -> flag=0.
        linhas_2025 = panel[(panel["ano"] == 2025) & (panel["trimestre"] < 4)]
        assert linhas_2025["flag_ano_eleitoral"].eq(0).all()
        # 2025-T4 prevê 2026-T1 (par) -> flag=1, não 0.
        linha_2025_t4 = panel[(panel["ano"] == 2025) & (panel["trimestre"] == 4)]
        assert linha_2025_t4["flag_ano_eleitoral"].eq(1).all()

    def test_lag_4_trimestres_has_real_value_across_year_boundary(self):
        """Prova a correção do bug: agrupar por `codigo_orgao` (estável) em vez
        de `sk_orgao` (= md5(codigo, ano), reseta todo ano) permite o lag de 4
        trimestres (mesmo trimestre do ano anterior) enxergar através da
        virada de ano. Antes da correção, isso era sempre NaN — 0 de 14.920
        linhas reais em produção (ver docs/06-analise-critica.md, item achado
        na comparação com o script da Fernanda)."""
        panel = pf.build_quarterly_panel(_pagamentos_df(anos=(2024, 2025)), _contratos_df()).sort_values(
            ["ano", "trimestre"]
        )
        q1_2025 = panel[(panel["ano"] == 2025) & (panel["trimestre"] == 1)].iloc[0]
        q1_2024 = panel[(panel["ano"] == 2024) & (panel["trimestre"] == 1)].iloc[0]
        assert pd.notna(q1_2025["lag_4_trimestres"])
        assert q1_2025["lag_4_trimestres"] == q1_2024["valor_trimestre"]

    def test_target_crosses_year_boundary_from_q4_to_next_q1(self):
        """Q4 de um ano deve enxergar Q1 do ano seguinte como alvo — com o bug
        do `sk_orgao` por ano, essa transição também ficava sempre NaN."""
        panel = pf.build_quarterly_panel(_pagamentos_df(anos=(2024, 2025)), _contratos_df()).sort_values(
            ["ano", "trimestre"]
        )
        q4_2024 = panel[(panel["ano"] == 2024) & (panel["trimestre"] == 4)].iloc[0]
        q1_2025 = panel[(panel["ano"] == 2025) & (panel["trimestre"] == 1)].iloc[0]
        assert pd.notna(q4_2024["target_proximo_trimestre"])
        assert q4_2024["target_proximo_trimestre"] == q1_2025["valor_trimestre"]

    def test_valor_contratado_ativo_covers_contract_signed_in_earlier_year(self):
        """Contrato vigente 2024-01-01 a 2025-12-31 precisa contar como ativo
        também nos trimestres de 2025, não só no ano em que foi assinado —
        com o bug do `sk_orgao` por ano, contratos "de outro ano" nunca
        batiam e o valor saía zerado (89,5% das linhas em produção, ver
        docs/06-analise-critica.md)."""
        panel = pf.build_quarterly_panel(_pagamentos_df(anos=(2024, 2025)), _contratos_df())
        linha_2025 = panel[panel["ano"] == 2025].iloc[0]
        assert linha_2025["valor_contratado_ativo"] == 100_000.0


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

    def test_quantile_predictions_are_never_negative(self):
        # Achado da análise crítica de 26/07/2026: para órgão com histórico
        # baixo/perto de zero, a regressão por quantil pode extrapolar abaixo
        # de zero — previsão de pagamento negativa não faz sentido.
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=("A", "B")), _contratos_df(orgaos=("A", "B")))
        X = pf.build_feature_matrix(panel)
        y = panel.loc[X.index, "target_proximo_trimestre"]
        train_idx = X.index[y.notna()]

        models = pf.train_models(X.loc[train_idx], y.loc[train_idx])
        preds = pf.predict_quantiles(models, X.loc[train_idx])

        assert (preds["p10"] >= 0).all()
        assert (preds["p50"] >= 0).all()
        assert (preds["p90"] >= 0).all()

    def test_forecast_next_quarter_targets_last_known_row_per_org(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=("A", "B")), _contratos_df(orgaos=("A", "B")))
        X = pf.build_feature_matrix(panel)
        y = panel.loc[X.index, "target_proximo_trimestre"]
        train_idx = X.index[y.notna()]

        models = pf.train_models(X.loc[train_idx], y.loc[train_idx])
        resultado = pf.forecast_next_quarter(panel, X, models)

        assert len(resultado) == 2  # 1 previsão por órgão (o último trimestre conhecido)
        assert set(resultado["codigo_orgao"]) == {"A", "B"}
        assert (resultado["valor_previsto_p10"] <= resultado["valor_previsto_p50"]).all()
        assert (resultado["valor_previsto_p50"] <= resultado["valor_previsto_p90"]).all()

    def test_evaluate_returns_holdout_metrics(self):
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=("A", "B")), _contratos_df(orgaos=("A", "B")))
        X = pf.build_feature_matrix(panel)
        metrics = pf.evaluate(panel, X)
        assert "mae_mediana" in metrics
        assert "cobertura_intervalo_80pct" in metrics
        assert metrics["mae_mediana"] >= 0


class TestTimeSeriesSplitsAndTuning:
    """Cobre time_series_splits/tune_hyperparameters — sem teste dedicado antes
    da auditoria de rigor científico de 30/07/2026, que encontrou o vazamento
    corrigido aqui: o trimestre usado por evaluate() como holdout final não
    pode aparecer como fold de validação do tuning."""

    def _panel_X_y(self, orgaos=("A", "B")):
        panel = pf.build_quarterly_panel(_pagamentos_df(orgaos=orgaos), _contratos_df(orgaos=orgaos))
        X = pf.build_feature_matrix(panel)
        y = panel.loc[X.index, "target_proximo_trimestre"]
        return panel, X, y

    def test_folds_never_train_on_data_from_the_validation_quarter_or_later(self):
        panel, X, _y = self._panel_X_y()
        folds = pf.time_series_splits(panel, X, n_folds=3)
        assert folds  # a fixture tem trimestre suficiente pra gerar folds
        for train_idx, valid_idx in folds:
            valid_quarters = panel.loc[valid_idx, ["ano", "trimestre"]].drop_duplicates()
            assert len(valid_quarters) == 1  # cada fold valida um único trimestre inteiro
            ano_v, trimestre_v = valid_quarters.iloc[0][["ano", "trimestre"]]

            train_quarters = panel.loc[train_idx, ["ano", "trimestre"]]
            treino_nao_e_anterior = (train_quarters["ano"] > ano_v) | (
                (train_quarters["ano"] == ano_v) & (train_quarters["trimestre"] >= trimestre_v)
            )
            assert not treino_nao_e_anterior.any()

    def test_excluir_ultimo_trimestre_reserva_o_trimestre_mais_recente(self):
        panel, X, _y = self._panel_X_y()
        com_ultimo = pf.time_series_splits(panel, X, n_folds=1, excluir_ultimo_trimestre=False)
        sem_ultimo = pf.time_series_splits(panel, X, n_folds=1, excluir_ultimo_trimestre=True)

        quarter_com = panel.loc[com_ultimo[0][1], ["ano", "trimestre"]].apply(tuple, axis=1).iloc[0]
        quarter_sem = panel.loc[sem_ultimo[0][1], ["ano", "trimestre"]].apply(tuple, axis=1).iloc[0]

        assert quarter_sem != quarter_com
        assert quarter_sem < quarter_com  # estritamente anterior

    def test_tuning_fold_nunca_e_o_mesmo_trimestre_do_holdout_final_de_evaluate(self):
        """Prova a correção do vazamento (item crítico da auditoria): o
        trimestre que `evaluate()` usa como holdout final nunca pode ter sido
        um fold de validação durante a busca de hiperparâmetros."""
        panel, X, y = self._panel_X_y()

        known = X.index[y.notna()]
        holdout_evaluate = panel.loc[known, ["ano", "trimestre"]].apply(tuple, axis=1).max()

        _melhor_params, trials = pf.tune_hyperparameters(panel, X, y, n_folds=2)
        assert trials

        folds_do_tuning = pf.time_series_splits(panel, X, n_folds=2, excluir_ultimo_trimestre=True)
        quarters_usados_no_tuning = {
            panel.loc[valid_idx, ["ano", "trimestre"]].apply(tuple, axis=1).iloc[0] for _, valid_idx in folds_do_tuning
        }
        assert holdout_evaluate not in quarters_usados_no_tuning

    def test_tune_hyperparameters_retorna_combinacao_do_grid_com_cobertura_por_fold(self):
        panel, X, y = self._panel_X_y()
        melhor_params, trials = pf.tune_hyperparameters(panel, X, y, n_folds=2)

        assert set(melhor_params) == {"n_estimators", "max_depth", "learning_rate"}
        assert any(all(t[k] == v for k, v in melhor_params.items()) for t in trials)
        assert all("coberturas_por_fold" in t and t["coberturas_por_fold"] for t in trials)
        assert all(0.0 <= c <= 1.0 for t in trials for c in t["coberturas_por_fold"])


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

    def test_contratos_query_joins_dim_orgao_for_stable_codigo(self):
        # codigo_orgao (não sk_orgao) — ver correção do bug no topo do módulo.
        assert "iceberg.gold.dim_orgao" in pf.CONTRATOS_VIGENCIA_QUERY
        assert "codigo_orgao" in pf.CONTRATOS_VIGENCIA_QUERY

    def test_pagamento_query_does_not_select_sk_orgao(self):
        # sk_orgao só é usado internamente pro JOIN; não sai no resultado —
        # evita reintroduzir por acidente o agrupamento pela chave errada.
        assert "f.sk_orgao\n" not in pf.PAGAMENTO_QUERY
        assert "SELECT\n    o.codigo AS codigo_orgao" in pf.PAGAMENTO_QUERY


class TestRunMlflowTracking:
    """`run()` não deve tocar Trino/MLflow/disco real nos testes — tudo mockado."""

    def _patched_run(self, tmp_path, **run_kwargs):
        pagamentos = _pagamentos_df(orgaos=("A", "B"))
        contratos = _contratos_df(orgaos=("A", "B"))
        with (
            patch.object(pf, "extract_pagamento_series", return_value=pagamentos),
            patch.object(pf, "extract_contratos_vigencia", return_value=contratos),
            patch.object(pf, "write_forecasts") as mock_write,
            patch.object(pf, "configure_mlflow") as mock_configure,
            patch.object(pf, "mlflow") as mock_mlflow,
            patch.object(pf, "ARTIFACT_PATH", str(tmp_path / "model.joblib")),
        ):
            resultado = pf.run(**run_kwargs)
        return resultado, mock_write, mock_configure, mock_mlflow

    def test_configures_mlflow_experiment_and_starts_run(self, tmp_path):
        _, _, mock_configure, mock_mlflow = self._patched_run(tmp_path)

        mock_configure.assert_called_once_with(pf.MLFLOW_EXPERIMENT)
        mock_mlflow.start_run.assert_called_once()

    def test_logs_params_holdout_metrics_and_artifact(self, tmp_path):
        _, _, _, mock_mlflow = self._patched_run(tmp_path)

        params = mock_mlflow.log_params.call_args[0][0]
        assert params["quantiles"] == pf.QUANTILES
        logged_metric_calls = [c.args[0] for c in mock_mlflow.log_metrics.call_args_list]
        assert any("mae_mediana" in metrics for metrics in logged_metric_calls)
        mock_mlflow.set_tag.assert_called_once_with("model_version", pf.MODEL_VERSION)
        mock_mlflow.log_artifact.assert_called_once_with(str(tmp_path / "model.joblib"))

    def test_persist_false_still_logs_but_skips_write_forecasts(self, tmp_path):
        _, mock_write, _, mock_mlflow = self._patched_run(tmp_path, persist=False)

        mock_write.assert_not_called()
        mock_mlflow.log_artifact.assert_called_once()
