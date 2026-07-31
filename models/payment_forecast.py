"""Modelo 2 — previsão do volume de pagamentos por órgão para o próximo
trimestre (XGBoost, regressão por quantil para o intervalo de confiança).

Lê `iceberg.gold.fato_ordem_bancaria` (pagamento efetivo ao credor, 3º
estágio da despesa), não `fato_empenho` (compromisso orçamentário).

Features: `lag_1_trimestre`/`lag_4_trimestres` do valor pago por órgão,
valor contratado com vigência ativa no trimestre, natureza da despesa
dominante (one-hot) e flag de ano eleitoral — calculada sobre o ano do
trimestre PREVISTO (`_next_quarter`), não o da linha atual, já que para
trimestre=4 o alvo cai no ano seguinte.

Agrupamento por `codigo_orgao`, nunca `sk_orgao`: `dim_orgao` é versionada
por `(codigo, ano)`, então o mesmo órgão físico tem `sk_orgao` diferente a
cada ano — agrupar por ele quebraria os lags e a vigência de contrato
plurianual entre anos. `sk_orgao` só aparece nas queries para o JOIN com
`dim_orgao`.

`tune_hyperparameters()` roda validação cruzada walk-forward
(`time_series_splits()`, cada fold treina só com trimestres estritamente
anteriores ao validado) sobre um grid pequeno, otimizando pinball loss —
métrica que penaliza sub/sobre-previsão por quantil, ao contrário do MAE
(que só avalia a mediana). Usa `excluir_ultimo_trimestre=True`: o trimestre
mais recente é reservado como holdout final de `evaluate()`, então não pode
também influenciar a escolha de hiperparâmetros.

`COBERTURA_MIN/MAX_ACEITAVEL` é um gate de qualidade: se a cobertura do
intervalo [p10,p90] no holdout final fugir muito do nominal ~80%, `run()`
loga ERROR e marca `alerta_qualidade` no MLflow — não bloqueia o pipeline
(não há canal de alerta automatizado no projeto).

`forecast_quarters_backtest()` gera previsão retroativa para os últimos
`N_BACKTEST_QUARTERS` trimestres já fechados, treinando cada um só com dado
anterior a ele — permite comparar previsto vs. realizado sem esperar um
trimestre novo fechar. Gravada junto com a previsão real (`FORECAST_TABLE`),
marcada por `is_backtest`; os dois conjuntos nunca se sobrepõem (um exige
alvo desconhecido, o outro exige alvo conhecido).

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

# Schema `ml`, não `gold` — saída de modelo, não modelo dimensional.
FORECAST_TABLE = "iceberg.ml.previsao_pagamento_orgao"
MODEL_VERSION = "xgboost_quantile_v3_tuned"
MLFLOW_EXPERIMENT = "payment_forecast"

TOP_MODALIDADES = 8
QUANTILES = (0.1, 0.5, 0.9)  # intervalo de confiança ~80% + mediana

DEFAULT_HYPERPARAMS = {"n_estimators": 200, "max_depth": 4, "learning_rate": 0.05}

# Grid pequeno — roda a cada dbt build da Gold, precisa ser rápido. Inclui
# DEFAULT_HYPERPARAMS como um dos candidatos.
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

# Faixa aceitável de cobertura do intervalo [p10,p90] (nominal ~80%) no
# holdout final — fora disso, os quantis provavelmente estão mal calibrados.
# Sem canal de alerta automatizado no projeto: só loga ERROR + tag no MLflow.
COBERTURA_MIN_ACEITAVEL = 0.5
COBERTURA_MAX_ACEITAVEL = 0.98

# Nº de trimestres já fechados que recebem previsão retroativa junto com a
# previsão real — sempre os últimos com alvo conhecido, avança sozinho
# conforme a Gold recebe trimestres novos. Ver `forecast_quarters_backtest`.
N_BACKTEST_QUARTERS = 2

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

# codigo_orgao, não sk_orgao — ver docstring do módulo.
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
    # Trino devolve decimal(15,2) como decimal.Decimal (dtype "object"), que o
    # XGBoost rejeita — converte pra float.
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

    panel = panel.sort_values(["codigo_orgao", "ano", "trimestre"]).reset_index(drop=True)
    panel["valor_contratado_ativo"] = _valor_contratado_ativo_por_org(
        contratos, panel[["codigo_orgao", "ano", "trimestre"]]
    )
    # Ano do trimestre previsto, não da linha atual — equivalente vetorizado
    # de _next_quarter: só trimestre=4 empurra o ano-alvo pro ano seguinte.
    ano_alvo = panel["ano"] + (panel["trimestre"] == 4).astype(int)
    panel["flag_ano_eleitoral"] = (ano_alvo % 2 == 0).astype(int)

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
    # Valor previsto não pode ser negativo — regressão por quantil pode
    # extrapolar abaixo de zero para órgãos com histórico perto de zero.
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


def _cobertura_intervalo(y_true: pd.Series, preds: pd.DataFrame) -> float:
    """% de linhas onde o valor real cai dentro de [p10, p90] — deveria rondar
    80% (o intervalo é nominal ~80% de confiança) se os quantis estiverem bem
    calibrados. Mesma fórmula usada em `evaluate()`, reaproveitada por fold em
    `tune_hyperparameters()`."""
    return float(((y_true >= preds["p10"]) & (y_true <= preds["p90"])).mean())


def time_series_splits(
    panel: pd.DataFrame, X: pd.DataFrame, n_folds: int = N_FOLDS_TUNING, excluir_ultimo_trimestre: bool = False
) -> list[tuple]:
    """Validação cruzada walk-forward: cada fold valida um trimestre inteiro,
    treinando só com trimestres estritamente anteriores (sem vazamento). Os
    últimos `n_folds` trimestres com alvo conhecido viram os folds.

    `excluir_ultimo_trimestre=True` remove o trimestre mais recente antes de
    montar os folds — usado por `tune_hyperparameters` para não deixar o
    holdout final de `evaluate()` também entrar na seleção de hiperparâmetros."""
    y = panel.loc[X.index, "target_proximo_trimestre"]
    known_mask = y.notna()

    quarters = panel.loc[X.index, ["ano", "trimestre"]]
    quarters_with_target = quarters[known_mask].drop_duplicates().sort_values(["ano", "trimestre"])
    quarter_list = list(quarters_with_target.itertuples(index=False, name=None))
    if excluir_ultimo_trimestre and quarter_list:
        quarter_list = quarter_list[:-1]
    fold_quarters = quarter_list[-n_folds:]

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
    `time_series_splits`), escolhendo pelo pinball loss médio (folds ×
    quantis). Retorna os hiperparâmetros vencedores e o log de todas as
    tentativas, para registrar no MLflow.

    Usa `excluir_ultimo_trimestre=True` — ver `evaluate()`/`run()`. Também
    registra a cobertura do intervalo por fold (não só o pinball loss), para
    checar se a calibração ~80% se sustenta em vários períodos, não só no
    holdout único de `evaluate()`."""
    folds = time_series_splits(panel, X, n_folds=n_folds, excluir_ultimo_trimestre=True)
    if not folds:
        logger.warning("Sem folds de validação temporal suficientes — mantendo hiperparâmetros padrão")
        return dict(DEFAULT_HYPERPARAMS), []

    trials: list[dict] = []
    for params in grid:
        fold_losses = []
        fold_coberturas = []
        for train_idx, valid_idx in folds:
            models = train_models(X.loc[train_idx], y.loc[train_idx], quantiles=quantiles, hyperparams=params)
            preds = predict_quantiles(models, X.loc[valid_idx])
            y_valid = y.loc[valid_idx]
            fold_coberturas.append(_cobertura_intervalo(y_valid, preds))
            loss = sum(_pinball_loss(y_valid, preds[f"p{int(q * 100)}"], q) for q in quantiles) / len(quantiles)
            fold_losses.append(loss)
        trials.append(
            {
                **params,
                "pinball_loss_medio": sum(fold_losses) / len(fold_losses),
                "n_folds": len(fold_losses),
                "coberturas_por_fold": fold_coberturas,
            }
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


def forecast_quarters_backtest(
    panel: pd.DataFrame,
    X: pd.DataFrame,
    y: pd.Series,
    hyperparams: dict,
    n_quarters: int = N_BACKTEST_QUARTERS,
    quantiles: tuple[float, ...] = QUANTILES,
) -> pd.DataFrame:
    """Previsão retroativa para os `n_quarters` trimestres mais recentes com
    alvo já conhecido — cada um treinado só com dado estritamente anterior a
    ele (sem vazamento), com os hiperparâmetros já escolhidos por
    `tune_hyperparameters`. Mesmo formato de `forecast_next_quarter`, mas sem
    sobreposição: aqui o alvo é conhecido, lá é desconhecido."""
    folds = time_series_splits(panel, X, n_folds=n_quarters)
    resultados = []
    for train_idx, valid_idx in folds:
        models = train_models(X.loc[train_idx], y.loc[train_idx], quantiles=quantiles, hyperparams=hyperparams)
        preds = predict_quantiles(models, X.loc[valid_idx])

        linhas = panel.loc[valid_idx, ["codigo_orgao", "nome_orgao", "ano", "trimestre"]].copy()
        linhas[["ano_previsto", "trimestre_previsto"]] = linhas.apply(
            lambda r: pd.Series(_next_quarter(int(r["ano"]), int(r["trimestre"]))), axis=1
        )
        linhas["valor_previsto_p10"] = preds["p10"].values
        linhas["valor_previsto_p50"] = preds["p50"].values
        linhas["valor_previsto_p90"] = preds["p90"].values
        resultados.append(linhas)

    colunas = [
        "codigo_orgao",
        "nome_orgao",
        "ano",
        "trimestre",
        "ano_previsto",
        "trimestre_previsto",
        "valor_previsto_p10",
        "valor_previsto_p50",
        "valor_previsto_p90",
    ]
    if not resultados:
        return pd.DataFrame(columns=colunas).drop(columns=["ano", "trimestre"])
    return pd.concat(resultados, ignore_index=True).drop(columns=["ano", "trimestre"])


def save_model(models: dict[float, XGBRegressor], feature_columns: list[str], path: str = ARTIFACT_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump({"models": models, "feature_columns": feature_columns}, path)
    logger.info("Modelo salvo em %s", path)


def load_model(path: str = ARTIFACT_PATH) -> tuple[dict[float, XGBRegressor], list[str]]:
    bundle = joblib.load(path)
    return bundle["models"], bundle["feature_columns"]


def write_forecasts(resultado: pd.DataFrame, model_version: str = MODEL_VERSION) -> None:
    """`resultado` precisa ter `is_backtest` (bool): False para a previsão real
    (`forecast_next_quarter`, alvo desconhecido), True para a retroativa
    (`forecast_quarters_backtest`, alvo já conhecido — ver constante
    `N_BACKTEST_QUARTERS`)."""
    payload = resultado[
        [
            "codigo_orgao",
            "nome_orgao",
            "ano_previsto",
            "trimestre_previsto",
            "valor_previsto_p10",
            "valor_previsto_p50",
            "valor_previsto_p90",
            "is_backtest",
        ]
    ].copy()
    payload["model_version"] = model_version
    payload["scored_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
            codigo_orgao varchar,
            nome_orgao varchar,
            ano_previsto integer,
            trimestre_previsto integer,
            valor_previsto_p10 double,
            valor_previsto_p50 double,
            valor_previsto_p90 double,
            is_backtest boolean,
            model_version varchar,
            scored_at timestamp
        )
    """
    # ADD COLUMN idempotente para ambientes onde a tabela já existia sem
    # `is_backtest` — `CREATE TABLE IF NOT EXISTS` só cobre ambiente do zero.
    trino_io.execute(f"ALTER TABLE IF EXISTS {FORECAST_TABLE} ADD COLUMN IF NOT EXISTS is_backtest boolean")

    trino_io.replace_table(
        table=FORECAST_TABLE,
        df=payload,
        columns=list(payload.columns),
        ddl=ddl,
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

        # Roda antes do log_params abaixo para logar os hiperparâmetros
        # vencedores, não os fixos.
        melhor_params, trials = tune_hyperparameters(panel, X, y)
        for i, trial in enumerate(trials):
            mlflow.log_metric(f"tuning_trial_{i}_pinball_loss", trial["pinball_loss_medio"])

        # Cobertura por fold da combinação vencedora — valida calibração em
        # vários trimestres, não só no holdout único de evaluate() abaixo.
        melhor_trial = min(trials, key=lambda t: t["pinball_loss_medio"]) if trials else {}
        coberturas_por_fold = melhor_trial.get("coberturas_por_fold", [])
        for i, cobertura_fold in enumerate(coberturas_por_fold):
            mlflow.log_metric(f"tuning_fold_{i}_cobertura_intervalo", cobertura_fold)
        if len(coberturas_por_fold) > 1:
            mlflow.log_metric("tuning_cobertura_desvio_padrao_entre_folds", float(pd.Series(coberturas_por_fold).std()))

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

        # Guarda de regressão: se lag_4_trimestres real (não o fallback) cair
        # a zero, o agrupamento por órgão voltou a usar uma chave que reseta
        # todo ano (ver docstring do módulo). Logado sempre, para comparar a
        # tendência entre runs no MLflow.
        pct_lag4_real = float(panel.loc[X.index, "lag_4_trimestres"].notna().mean()) if len(X) else 0.0
        mlflow.log_metric("pct_lag_4_trimestres_com_dado_real", pct_lag4_real)
        if pct_lag4_real == 0.0:
            logger.warning(
                "lag_4_trimestres sem NENHUM valor real neste run (100%% fallback pra lag_1_trimestre) — "
                "suspeita de regressão do bug de agrupamento por sk_orgao versionado por ano."
            )

        # Holdout = trimestre reservado por excluir_ultimo_trimestre=True no
        # tuning acima — nunca usado pra escolher hiperparâmetros.
        metrics = evaluate(panel, X, hyperparams=melhor_params)
        if metrics:
            mlflow.log_metrics(metrics)
            cobertura = metrics["cobertura_intervalo_80pct"]
            if not (COBERTURA_MIN_ACEITAVEL <= cobertura <= COBERTURA_MAX_ACEITAVEL):
                logger.error(
                    "ALERTA DE QUALIDADE: cobertura do intervalo no holdout (%.1f%%) fora da faixa "
                    "aceitável [%.0f%%, %.0f%%] — os quantis deste treino provavelmente estão mal "
                    "calibrados. Revisar antes de confiar nas previsões gravadas.",
                    cobertura * 100,
                    COBERTURA_MIN_ACEITAVEL * 100,
                    COBERTURA_MAX_ACEITAVEL * 100,
                )
                mlflow.set_tag("alerta_qualidade", "cobertura_fora_da_faixa_aceitavel")

        train_idx = X.index[y.notna()]
        models = train_models(X.loc[train_idx], y.loc[train_idx], hyperparams=melhor_params)
        save_model(models, list(X.columns))

        resultado = forecast_next_quarter(panel, X, models)
        resultado["is_backtest"] = False

        resultado_backtest = forecast_quarters_backtest(panel, X, y, melhor_params)
        resultado_backtest["is_backtest"] = True

        mlflow.log_metric("n_orgaos_previstos", float(len(resultado)))
        mlflow.log_metric("n_linhas_previsao_retroativa", float(len(resultado_backtest)))
        logger.info(
            "Previsão concluída: %s órgãos (previsão real, alvo desconhecido), %s linhas de previsão "
            "retroativa (últimos %s trimestres fechados), mae_mediana(holdout)=%s",
            len(resultado),
            len(resultado_backtest),
            N_BACKTEST_QUARTERS,
            metrics.get("mae_mediana"),
        )
        if persist:
            write_forecasts(pd.concat([resultado, resultado_backtest], ignore_index=True))
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
