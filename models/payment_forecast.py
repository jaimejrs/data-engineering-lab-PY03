"""Modelo 2 — previsão do volume de pagamentos por órgão para o próximo trimestre
(XGBoost, regressão por quantil para o intervalo de confiança).

Lê `iceberg.gold.fato_ordem_bancaria` — o pagamento efetivo ao credor (3º
estágio da despesa: contrato → empenho → ordem bancária), criado em
24/07/2026 (ver `docs/06-analise-critica.md`, item 3). Antes desse fato
existir na Gold, este módulo usava `fato_empenho` (estágio de **compromisso**
orçamentário, não o desembolso) como proxy — trocar foi só mudar a query de
origem, o resto do pipeline de features não mudou.

Features (conforme docs/Trabalho Final.pdf, seção 4.3 — Modelo 2), adaptadas ao
que existe na Gold:
  - histórico trimestral de pagamentos -> `lag_1_trimestre` (trimestre anterior)
    e `lag_4_trimestres` (mesmo trimestre do ano anterior), por órgão
  - valor contratado ativo -> soma de `valor_contrato` com vigência sobrepondo o
    trimestre (via `fato_contrato` + `iceberg.silver.contratos`)
  - tipo de despesa -> `natureza` da ordem bancária (classificação orçamentária
    padrão, ex: "3.3.90.18" — mais fiel ao pedido do enunciado do que a
    modalidade de contratação usada quando o proxy era `fato_empenho`; one-hot
    das mais frequentes)
  - ano eleitoral -> `ano % 2 == 0` (simplificação: no Brasil todo ano par tem
    eleição, municipal ou estadual/federal, alternando)

BUG corrigido (25/07/2026, achado comparando com o script independente da
Fernanda, dona da Fase 3/ML — ela chegou no mesmo problema por outro caminho,
usando Prophet em vez de XGBoost, e documentou a correção no dela):
`dim_orgao.sk_orgao = md5(codigo, ano)` — o MESMO órgão físico tem um
`sk_orgao` diferente a cada ano. Agrupar a série temporal por `sk_orgao`
(como este módulo fazia) reseta o grupo todo ano, então:
  - `lag_4_trimestres` (mesmo trimestre do ano anterior) NUNCA tinha um valor
    real — confirmado contra produção: 0 de 14.920 linhas com o campo
    preenchido antes do fallback. Virava sempre uma cópia de `lag_1_trimestre`.
  - `valor_contratado_ativo` só contava contratos assinados no MESMO ano do
    trimestre sendo avaliado (porque a comparação também usava `sk_orgao`) —
    89,5% das linhas saíam com o valor zerado, mesmo tendo contrato
    plurianual vigente.
Corrigido: todo agrupamento/junção por órgão agora usa `codigo_orgao` (estável
no tempo), nunca `sk_orgao`. `sk_orgao` só aparece nas queries para fazer o
JOIN com `dim_orgao`, não sai mais no resultado.

Hiperparâmetros (revisão de 30/07/2026, docs/06-analise-critica.md):
`n_estimators=200/max_depth=4/learning_rate=0.05` eram valores padrão
razoáveis, mas nunca tinham passado por busca real — só um holdout único
(último trimestre conhecido), que não valida hiperparâmetros de forma
confiável em série temporal. `tune_hyperparameters()` roda validação cruzada
walk-forward (`time_series_splits()`: cada fold valida um trimestre inteiro
treinando só com trimestres estritamente anteriores, sem vazamento) sobre um
grid pequeno, escolhendo pelo pinball loss médio (a métrica correta pra
regressão por quantil — MAE só avalia a mediana, ignora se p10/p90 estão bem
calibrados). Chamado a cada `run()`, antes do treino final; os resultados de
cada combinação testada vão pro MLflow junto com a escolhida.

Uso: python -m models.payment_forecast
"""

import argparse
import logging
import os

import joblib
import mlflow
import pandas as pd
from xgboost import XGBRegressor

from src import trino_io
from src.mlflow_utils import configure_mlflow

logger = logging.getLogger(__name__)

ARTIFACT_PATH = os.environ.get(
    "FORECAST_MODEL_PATH",
    os.path.join(os.path.dirname(__file__), "artifacts", "xgboost_previsao_pagamentos.joblib"),
)

# Schema `ml` (não `gold` — separado do modelo dimensional desde 26/07/2026).
FORECAST_TABLE = "iceberg.ml.previsao_pagamento_orgao"
MODEL_VERSION = "xgboost_quantile_v3_tuned"
MLFLOW_EXPERIMENT = "payment_forecast"

TOP_MODALIDADES = 8
QUANTILES = (0.1, 0.5, 0.9)  # intervalo de confiança ~80% + mediana

DEFAULT_HYPERPARAMS = {"n_estimators": 200, "max_depth": 4, "learning_rate": 0.05}

# Grid pequeno de propósito — o painel roda a cada novo `dbt build` da Gold
# (ver dags/dag_ml_inference.py), então a busca precisa ser rápida. Inclui o
# ponto DEFAULT_HYPERPARAMS (linha 4) como referência de comparação.
HYPERPARAM_GRID: list[dict] = [
    {"n_estimators": 100, "max_depth": 3, "learning_rate": 0.1},
    {"n_estimators": 100, "max_depth": 4, "learning_rate": 0.05},
    {"n_estimators": 200, "max_depth": 3, "learning_rate": 0.05},
    {"n_estimators": 200, "max_depth": 4, "learning_rate": 0.05},
    {"n_estimators": 200, "max_depth": 5, "learning_rate": 0.05},
    {"n_estimators": 300, "max_depth": 4, "learning_rate": 0.03},
    {"n_estimators": 300, "max_depth": 4, "learning_rate": 0.1},
]

N_FOLDS_TUNING = 4

PAGAMENTO_QUERY = """
SELECT
    o.codigo AS codigo_orgao,
    o.nome AS nome_orgao,
    f.valor,
    f.natureza AS modalidade,
    t.ano,
    t.trimestre
FROM iceberg.gold.fato_ordem_bancaria f
JOIN iceberg.gold.dim_tempo t ON f.sk_tempo = t.sk_tempo
JOIN iceberg.gold.dim_orgao o ON f.sk_orgao = o.sk_orgao
WHERE f.valor IS NOT NULL AND f.sk_orgao IS NOT NULL AND NOT f.flag_cancelada
"""

# `codigo_orgao` (não `sk_orgao`) — sk_orgao do contrato reflete o ANO EM QUE
# ELE FOI ASSINADO (dim_orgao é versionada por (codigo, ano)); um contrato
# plurianual assinado em 2023 e ainda vigente em 2026 tem sk_orgao "de 2023",
# que nunca bateria com o sk_orgao "de 2026" do trimestre sendo avaliado. Ver
# nota de correção do bug no topo do módulo.
CONTRATOS_VIGENCIA_QUERY = """
SELECT
    o.codigo AS codigo_orgao,
    f.valor_contrato,
    sc.data_inicio,
    sc.data_termino
FROM iceberg.gold.fato_contrato f
JOIN iceberg.gold.dim_orgao o ON o.sk_orgao = f.sk_orgao
JOIN iceberg.silver.contratos sc ON CAST(sc.id AS VARCHAR) = f.id_contrato_origem
WHERE f.sk_orgao IS NOT NULL AND f.valor_contrato IS NOT NULL
"""

_QUARTER_MONTH_END = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
_QUARTER_MONTH_START = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}


def extract_pagamento_series() -> pd.DataFrame:
    return trino_io.query(PAGAMENTO_QUERY)


def extract_contratos_vigencia() -> pd.DataFrame:
    return trino_io.query(CONTRATOS_VIGENCIA_QUERY)


def _quarter_bounds(ano: int, trimestre: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    inicio = pd.Timestamp(f"{ano}-{_QUARTER_MONTH_START[trimestre]}")
    fim = pd.Timestamp(f"{ano}-{_QUARTER_MONTH_END[trimestre]}")
    return inicio, fim


def _next_quarter(ano: int, trimestre: int) -> tuple[int, int]:
    return (ano + 1, 1) if trimestre == 4 else (ano, trimestre + 1)


def _valor_contratado_ativo_por_org(contratos: pd.DataFrame, quarters_por_org: pd.DataFrame) -> pd.Series:
    """Para cada linha `(codigo_orgao, ano, trimestre)`, soma `valor_contrato`
    dos contratos daquele órgão cuja vigência sobrepõe o trimestre. Filtra por
    órgão antes de comparar datas — cada grupo tem poucos contratos, então o
    laço por órgão é barato mesmo com o dataset inteiro carregado em memória.

    Agrupa por `codigo_orgao` (estável no tempo), não por `sk_orgao` (que
    muda a cada ano — ver nota de correção do bug no topo do módulo): um
    contrato plurianual precisa continuar "ativo" nos trimestres de anos
    seguintes ao de sua assinatura.
    """
    inicio = pd.to_datetime(contratos["data_inicio"], errors="coerce")
    termino = pd.to_datetime(contratos["data_termino"], errors="coerce")
    c = contratos.assign(_inicio=inicio, _termino=termino)

    resultado = pd.Series(0.0, index=quarters_por_org.index)
    for codigo_orgao, idx in quarters_por_org.groupby("codigo_orgao").groups.items():
        grupo_contratos = c[c["codigo_orgao"] == codigo_orgao]
        if grupo_contratos.empty:
            continue
        for i in idx:
            q_inicio, q_fim = _quarter_bounds(quarters_por_org.at[i, "ano"], quarters_por_org.at[i, "trimestre"])
            ativo = grupo_contratos[
                (grupo_contratos["_inicio"].isna() | (grupo_contratos["_inicio"] <= q_fim))
                & (grupo_contratos["_termino"].isna() | (grupo_contratos["_termino"] >= q_inicio))
            ]
            resultado.at[i] = ativo["valor_contrato"].sum()
    return resultado


def build_quarterly_panel(pagamentos: pd.DataFrame, contratos: pd.DataFrame) -> pd.DataFrame:
    """Monta o painel órgão × trimestre com o alvo (valor do trimestre) e as
    features de previsão para o **próximo** trimestre.
    """
    # `valor`/`valor_contrato` vêm do Trino como decimal(15,2) -> o driver
    # devolve `decimal.Decimal` (dtype "object" no pandas), que o XGBoost
    # rejeita ("DataFrame.dtypes for data must be int, float, bool or
    # category"). Só apareceu rodando contra o Trino real — os testes usam
    # DataFrame sintético já em float.
    pagamentos = pagamentos.assign(valor=pd.to_numeric(pagamentos["valor"], errors="coerce"))
    contratos = contratos.assign(valor_contrato=pd.to_numeric(contratos["valor_contrato"], errors="coerce"))

    base = (
        pagamentos.groupby(["codigo_orgao", "nome_orgao", "ano", "trimestre"])
        .agg(valor_trimestre=("valor", "sum"))
        .reset_index()
    )

    top_mod = pagamentos["modalidade"].value_counts().head(TOP_MODALIDADES).index
    modalidade_dominante = (
        pagamentos.assign(modalidade=pagamentos["modalidade"].where(pagamentos["modalidade"].isin(top_mod), "OUTROS"))
        .groupby(["codigo_orgao", "ano", "trimestre"])["modalidade"]
        .agg(lambda s: s.value_counts().idxmax())
        .reset_index()
    )
    panel = base.merge(modalidade_dominante, on=["codigo_orgao", "ano", "trimestre"], how="left")

    # Agrupar por `codigo_orgao` (não `sk_orgao`, que muda a cada ano) é o que
    # faz os lags abaixo enxergarem o histórico plurianual do órgão — ver nota
    # de correção do bug no topo do módulo.
    panel = panel.sort_values(["codigo_orgao", "ano", "trimestre"]).reset_index(drop=True)
    panel["valor_contratado_ativo"] = _valor_contratado_ativo_por_org(
        contratos, panel[["codigo_orgao", "ano", "trimestre"]]
    )
    panel["flag_ano_eleitoral"] = (panel["ano"] % 2 == 0).astype(int)

    grouped = panel.groupby("codigo_orgao")["valor_trimestre"]
    panel["lag_1_trimestre"] = grouped.shift(1)
    panel["lag_4_trimestres"] = grouped.shift(4)
    # Alvo: valor do PRÓXIMO trimestre do mesmo órgão (o que o modelo aprende a prever).
    panel["target_proximo_trimestre"] = grouped.shift(-1)

    return panel


def build_feature_matrix(panel: pd.DataFrame) -> pd.DataFrame:
    """Converte o painel em matriz numérica para o XGBoost — exige `lag_1_trimestre`
    presente (sem histórico mínimo, a linha não tem o que prever a partir de)."""
    df = panel[panel["lag_1_trimestre"].notna()].copy()

    out = pd.DataFrame(index=df.index)
    out["lag_1_trimestre"] = df["lag_1_trimestre"]
    out["lag_4_trimestres"] = df["lag_4_trimestres"].fillna(df["lag_1_trimestre"])
    out["valor_contratado_ativo"] = df["valor_contratado_ativo"].fillna(0.0)
    out["flag_ano_eleitoral"] = df["flag_ano_eleitoral"]
    out["trimestre"] = df["trimestre"]
    out = out.join(pd.get_dummies(df["modalidade"].fillna("DESCONHECIDA"), prefix="modalidade"))
    return out


def train_models(
    X: pd.DataFrame,
    y: pd.Series,
    quantiles: tuple[float, ...] = QUANTILES,
    hyperparams: dict | None = None,
) -> dict[float, XGBRegressor]:
    """Um `XGBRegressor` por quantil (`reg:quantileerror`) — dá a mediana (p50)
    como previsão central e p10/p90 como intervalo de confiança ~80%.

    `hyperparams` (n_estimators/max_depth/learning_rate) vem de
    `tune_hyperparameters()` quando chamado por `run()`; default
    `DEFAULT_HYPERPARAMS` quando chamado direto (ex: nos testes)."""
    params = {**DEFAULT_HYPERPARAMS, **(hyperparams or {})}
    models: dict[float, XGBRegressor] = {}
    for q in quantiles:
        model = XGBRegressor(
            objective="reg:quantileerror",
            quantile_alpha=q,
            random_state=42,
            **params,
        )
        model.fit(X, y)
        models[q] = model
    return models


def predict_quantiles(models: dict[float, XGBRegressor], X: pd.DataFrame) -> pd.DataFrame:
    preds = {f"p{int(q * 100)}": models[q].predict(X) for q in models}
    out = pd.DataFrame(preds, index=X.index)
    # Garante monotonicidade (p10 <= p50 <= p90) mesmo se os modelos discordarem
    # em algum ponto — comum em regressão por quantil treinada independentemente.
    cols = sorted(out.columns, key=lambda c: int(c[1:]))
    out[cols] = out[cols].cummax(axis=1)
    # Valor previsto de pagamento não pode ser negativo — para órgão/trimestre
    # com histórico baixo/perto de zero, a regressão por quantil pode
    # extrapolar abaixo de zero (achado da análise crítica de 26/07/2026).
    out[cols] = out[cols].clip(lower=0)
    return out


def evaluate(panel: pd.DataFrame, X: pd.DataFrame, hyperparams: dict | None = None) -> dict[str, float]:
    """Holdout temporal: o último trimestre com alvo conhecido vira teste; o
    resto treina. Métrica: MAE da mediana + cobertura do intervalo [p10, p90]
    (deveria rondar 80% se os quantis estiverem bem calibrados)."""
    y = panel.loc[X.index, "target_proximo_trimestre"]
    known = X.index[y.notna()]
    if len(known) < 20:
        logger.warning("Poucas linhas com alvo conhecido (%s) — avaliação pouco confiável", len(known))

    ultimo_trimestre = panel.loc[known, ["ano", "trimestre"]].apply(tuple, axis=1).max()
    is_holdout = panel.loc[known, ["ano", "trimestre"]].apply(tuple, axis=1) == ultimo_trimestre
    holdout_idx = known[is_holdout.values]
    train_idx = known[~is_holdout.values]

    if len(holdout_idx) == 0 or len(train_idx) == 0:
        logger.warning("Sem separação treino/holdout válida — pulando avaliação")
        return {}

    models_eval = train_models(X.loc[train_idx], y.loc[train_idx], hyperparams=hyperparams)
    preds = predict_quantiles(models_eval, X.loc[holdout_idx])
    y_true = y.loc[holdout_idx]

    mae = (preds["p50"] - y_true).abs().mean()
    cobertura = ((y_true >= preds["p10"]) & (y_true <= preds["p90"])).mean()
    metrics = {"mae_mediana": float(mae), "cobertura_intervalo_80pct": float(cobertura), "n_holdout": len(holdout_idx)}
    logger.info("Avaliação (holdout=último trimestre conhecido): %s", metrics)
    return metrics


def _pinball_loss(y_true: pd.Series, y_pred: pd.Series, quantile: float) -> float:
    """Métrica correta pra regressão por quantil (função de perda que o
    próprio `reg:quantileerror` otimiza) — penaliza sub-previsão e
    sobre-previsão de forma assimétrica conforme o quantil, ao contrário do
    MAE (que só faz sentido pra medir o p50)."""
    delta = y_true - y_pred
    return float((delta.clip(lower=0) * quantile + (-delta).clip(lower=0) * (1 - quantile)).mean())


def time_series_splits(panel: pd.DataFrame, X: pd.DataFrame, n_folds: int = N_FOLDS_TUNING) -> list[tuple]:
    """Validação cruzada walk-forward: cada fold valida UM trimestre inteiro
    (todas as linhas daquele ano/trimestre com alvo conhecido), treinando só
    com linhas de trimestres estritamente anteriores — sem vazamento
    temporal (nenhum fold treina com dado "do futuro" em relação ao que
    valida). Os últimos `n_folds` trimestres com alvo conhecido viram os
    folds de validação; o resto de cada um vira o treino daquele fold."""
    y = panel.loc[X.index, "target_proximo_trimestre"]
    known_mask = y.notna()

    quarters = panel.loc[X.index, ["ano", "trimestre"]]
    quarters_with_target = quarters[known_mask].drop_duplicates().sort_values(["ano", "trimestre"])
    fold_quarters = list(quarters_with_target.itertuples(index=False, name=None))[-n_folds:]

    folds = []
    for ano_q, trimestre_q in fold_quarters:
        is_this_quarter = (quarters["ano"] == ano_q) & (quarters["trimestre"] == trimestre_q)
        valid_idx = X.index[is_this_quarter & known_mask]

        is_earlier = (quarters["ano"] < ano_q) | ((quarters["ano"] == ano_q) & (quarters["trimestre"] < trimestre_q))
        train_idx = X.index[is_earlier & known_mask]

        if len(valid_idx) and len(train_idx):
            folds.append((train_idx, valid_idx))
    return folds


def tune_hyperparameters(
    panel: pd.DataFrame,
    X: pd.DataFrame,
    y: pd.Series,
    grid: list[dict] = HYPERPARAM_GRID,
    n_folds: int = N_FOLDS_TUNING,
    quantiles: tuple[float, ...] = QUANTILES,
) -> tuple[dict, list[dict]]:
    """Busca de hiperparâmetros com validação cruzada temporal (ver
    `time_series_splits`) — escolhe pelo pinball loss médio (folds × quantis).
    Retorna os hiperparâmetros vencedores e o log de todas as tentativas
    (pra registrar no MLflow, permitindo auditar a escolha depois)."""
    folds = time_series_splits(panel, X, n_folds=n_folds)
    if not folds:
        logger.warning("Sem folds de validação temporal suficientes — mantendo hiperparâmetros padrão")
        return dict(DEFAULT_HYPERPARAMS), []

    trials: list[dict] = []
    for params in grid:
        fold_losses = []
        for train_idx, valid_idx in folds:
            models = train_models(X.loc[train_idx], y.loc[train_idx], quantiles=quantiles, hyperparams=params)
            preds = predict_quantiles(models, X.loc[valid_idx])
            y_valid = y.loc[valid_idx]
            loss = sum(_pinball_loss(y_valid, preds[f"p{int(q * 100)}"], q) for q in quantiles) / len(quantiles)
            fold_losses.append(loss)
        trials.append(
            {**params, "pinball_loss_medio": sum(fold_losses) / len(fold_losses), "n_folds": len(fold_losses)}
        )

    melhor = min(trials, key=lambda t: t["pinball_loss_medio"])
    melhor_params = {k: melhor[k] for k in ("n_estimators", "max_depth", "learning_rate")}
    logger.info(
        "Melhor combinação (walk-forward CV, %s folds, %s candidatos): %s (pinball_loss_medio=%.2f)",
        len(folds),
        len(grid),
        melhor_params,
        melhor["pinball_loss_medio"],
    )
    return melhor_params, trials


def forecast_next_quarter(panel: pd.DataFrame, X: pd.DataFrame, models: dict[float, XGBRegressor]) -> pd.DataFrame:
    """Previsão real: linhas cujo alvo é desconhecido (o trimestre mais recente
    de cada órgão) — é para elas que ainda não existe dado real do próximo
    trimestre."""
    to_predict = panel.loc[X.index][panel.loc[X.index, "target_proximo_trimestre"].isna()]
    preds = predict_quantiles(models, X.loc[to_predict.index])

    resultado = to_predict[["codigo_orgao", "nome_orgao", "ano", "trimestre"]].copy()
    resultado[["ano_previsto", "trimestre_previsto"]] = resultado.apply(
        lambda r: pd.Series(_next_quarter(int(r["ano"]), int(r["trimestre"]))), axis=1
    )
    resultado["valor_previsto_p10"] = preds["p10"].values
    resultado["valor_previsto_p50"] = preds["p50"].values
    resultado["valor_previsto_p90"] = preds["p90"].values
    return resultado.reset_index(drop=True)


def save_model(models: dict[float, XGBRegressor], feature_columns: list[str], path: str = ARTIFACT_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump({"models": models, "feature_columns": feature_columns}, path)
    logger.info("Modelo salvo em %s", path)


def load_model(path: str = ARTIFACT_PATH) -> tuple[dict[float, XGBRegressor], list[str]]:
    bundle = joblib.load(path)
    return bundle["models"], bundle["feature_columns"]


def write_forecasts(resultado: pd.DataFrame, model_version: str = MODEL_VERSION) -> None:
    payload = resultado[
        [
            "codigo_orgao",
            "nome_orgao",
            "ano_previsto",
            "trimestre_previsto",
            "valor_previsto_p10",
            "valor_previsto_p50",
            "valor_previsto_p90",
        ]
    ].copy()
    payload["model_version"] = model_version
    payload["scored_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    trino_io.replace_table(
        table=FORECAST_TABLE,
        df=payload,
        columns=list(payload.columns),
        ddl=f"""
            CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
                codigo_orgao varchar,
                nome_orgao varchar,
                ano_previsto integer,
                trimestre_previsto integer,
                valor_previsto_p10 double,
                valor_previsto_p50 double,
                valor_previsto_p90 double,
                model_version varchar,
                scored_at timestamp
            )
        """,
        casts={"scored_at": "TIMESTAMP"},
    )
    logger.info("Gravadas %s previsões em %s", len(payload), FORECAST_TABLE)


def run(persist: bool = True) -> pd.DataFrame:
    """Monta o painel, treina os 3 regressores por quantil e prevê o próximo
    trimestre por órgão (por padrão, grava em `FORECAST_TABLE`).

    Cada chamada é uma run do MLflow (tarefa 29) — params dos quantis/hiper-
    parâmetros, métricas de holdout (MAE, cobertura do intervalo) e o `.joblib`
    dos 3 modelos como artefato.
    """
    configure_mlflow(MLFLOW_EXPERIMENT)
    with mlflow.start_run(run_name=f"{MODEL_VERSION}_{pd.Timestamp.utcnow():%Y%m%d_%H%M%S}"):
        pagamentos = extract_pagamento_series()
        contratos = extract_contratos_vigencia()
        panel = build_quarterly_panel(pagamentos, contratos)
        X = build_feature_matrix(panel)
        y = panel.loc[X.index, "target_proximo_trimestre"]

        # Busca de hiperparâmetros com validação cruzada temporal (ver
        # "Hiperparâmetros" no topo do módulo) — roda ANTES do log_params
        # principal pra logar os hiperparâmetros vencedores, não os fixos.
        melhor_params, trials = tune_hyperparameters(panel, X, y)
        for i, trial in enumerate(trials):
            mlflow.log_metric(f"tuning_trial_{i}_pinball_loss", trial["pinball_loss_medio"])

        mlflow.log_params(
            {
                "quantiles": QUANTILES,
                **melhor_params,
                "n_orgaos_trimestres": len(X),
                "n_features": X.shape[1],
                "hyperparam_tuning_n_trials": len(trials),
                "hyperparam_tuning_n_folds": N_FOLDS_TUNING,
            }
        )
        mlflow.set_tag("model_version", MODEL_VERSION)

        # Guarda de regressão para o bug do sk_orgao-por-ano (ver topo do
        # módulo): se `lag_4_trimestres` real (não o fallback) cair a ~0 de
        # novo, algum código voltou a agrupar por uma chave que reseta a cada
        # ano. Logado sempre, não só quando falha, pra dar pra comparar a
        # tendência entre runs no MLflow.
        pct_lag4_real = float(panel.loc[X.index, "lag_4_trimestres"].notna().mean()) if len(X) else 0.0
        mlflow.log_metric("pct_lag_4_trimestres_com_dado_real", pct_lag4_real)
        if pct_lag4_real == 0.0:
            logger.warning(
                "lag_4_trimestres sem NENHUM valor real neste run (100%% fallback pra lag_1_trimestre) — "
                "suspeita de regressão do bug de agrupamento por sk_orgao versionado por ano."
            )

        # holdout = último trimestre com alvo conhecido, já com os hiperparâmetros vencedores
        metrics = evaluate(panel, X, hyperparams=melhor_params)
        if metrics:
            mlflow.log_metrics(metrics)

        train_idx = X.index[y.notna()]
        models = train_models(X.loc[train_idx], y.loc[train_idx], hyperparams=melhor_params)
        save_model(models, list(X.columns))

        resultado = forecast_next_quarter(panel, X, models)
        mlflow.log_metric("n_orgaos_previstos", float(len(resultado)))
        logger.info(
            "Previsão concluída: %s órgãos, mae_mediana(holdout)=%s",
            len(resultado),
            metrics.get("mae_mediana"),
        )
        if persist:
            write_forecasts(resultado)
        mlflow.log_artifact(ARTIFACT_PATH)
    return resultado


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Modelo 2 — previsão trimestral de pagamentos por órgão")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    _parse_args()
    resultado = run()
    print(resultado.sort_values("valor_previsto_p50", ascending=False).head(20).to_string(index=False))
