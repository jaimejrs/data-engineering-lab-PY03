"""Modelo 1 — detecção de anomalias em contratos públicos (Isolation Forest).

Aprende o comportamento "normal" de um contrato (valor, modalidade, tipo de
objeto, duração, emergência, histórico de infração do credor) e atribui um
score de anomalia em [0, 1] — quanto mais alto, mais atípico. Não supervisionado
por decisão de projeto (não existe rótulo de "contrato irregular" em nenhuma das
fontes — ver docs/Trabalho Final.pdf, seção 7.3: "avalie o modelo com analistas,
pois não há rótulos ground-truth").

Features (conforme docs/Trabalho Final.pdf, seção 4.3 — Modelo 1):
  - valor_contrato
  - modalidade (one-hot)
  - tipo_objeto (one-hot; categorias raras agrupadas em "OUTROS")
  - dias_vigencia (data_termino - data_inicio)
  - flag_emergency
  - historico_credor_infringement

Lê a Gold (`iceberg.gold.fato_contrato` + dimensões) e a Silver
(`iceberg.silver.contratos`, só para `tipo_objeto`/vigência, que ainda não estão
modelados na Gold) via Trino — mesmo padrão de conexão dos notebooks de EDA.

Desbalanceamento (tarefa 28, docs/Trabalho Final.pdf seção 7.3: "Trate o
desbalanceamento [...] Use SMOTE ou ajuste de pesos caso converta para
classificação supervisionada"): SMOTE/ajuste de peso por classe são técnicas
de aprendizado SUPERVISIONADO — pressupõem rótulo (0/1) pra saber o que é
minoria. Este modelo é deliberadamente não supervisionado (sem rótulo de
"contrato irregular" em nenhuma fonte), então não há classe pra rebalancear
no treino. O equivalente correto aqui é outro: calibrar o PONTO DE CORTE em
produção a partir da distribuição real do score, não do treino —
`summarize_score_distribution()` expõe percentis e contagem de contratos
acima de thresholds usuais (0.7/0.8/0.9/0.95), logados no MLflow a cada run,
pra quem for operacionalizar (Fernanda/analistas) escolher um limiar
compatível com a capacidade real de revisão manual, em vez de um corte
arbitrário embutido no código.

Hiperparâmetros (revisão de 30/07/2026, docs/06-analise-critica.md):
  - `contamination`: só afeta `flag_anomalia` (o corte binário do `predict()`
    do próprio Isolation Forest) — verificado experimentalmente que variar
    contamination desloca `decision_function()` por uma CONSTANTE (`offset_`),
    sem mudar o ranking relativo dos contratos (correlação de Spearman = 1.0
    entre contamination=0.01/0.05/0.1/'auto' num teste controlado). Como
    `score_anomalia()` normaliza com MinMaxScaler logo em seguida, esse
    deslocamento constante desaparece — `contamination` não tem NENHUM efeito
    sobre o score contínuo que o painel usa para priorizar revisão. Não há
    o que "otimizar" aqui sem rótulo; manter 'auto' (default do scikit-learn)
    é o correto.
  - `n_estimators`: este SIM afeta o score contínuo — mais árvores reduz a
    variância da estimativa. Testado sobre os ~216 mil contratos reais de
    produção, re-treinando com seeds diferentes e medindo a interseção do
    "top 1% mais atípico" entre execuções (a métrica que importa: quem entra
    na lista de revisão prioritária não pode depender de sorte do random_state):
    100 árvores → 80,8% de interseção; 200 (valor anterior) → 87,6%; 400 →
    92,6%; 600 → 94,3% (retornos decrescentes a partir daqui). Subido para
    400 — dobra o custo de treino em troca de uma redução real na variância
    de quem aparece como prioridade de revisão.

Gate de qualidade em produção (auditoria de rigor científico, 30/07/2026):
sem rótulo não dá pra medir acurácia, mas dá pra detectar sinais de que algo
quebrou no pipeline:
  - Distribuição degenerada: se `score_p50 == score_p99`, o modelo não está
    discriminando NENHUM contrato neste treino (ex: feature matrix quebrada,
    todas as linhas idênticas) — sinal de que o score inteiro da execução não
    é confiável.
  - Deriva vs. execução anterior: `score_medio` costuma variar pouco entre
    re-treinos sobre dados parecidos (~0,165-0,168 nas últimas execuções
    reais). Uma mudança grande demais (`DRIFT_SCORE_MEDIO_MAX_ACEITAVEL`)
    sinaliza possível regressão a montante (bug de feature, mudança não
    intencional nos dados de entrada) — consultado ANTES de sobrescrever
    `SCORE_TABLE`, comparado contra a média que estava valendo em produção.
Nenhum dos dois bloqueia o pipeline (sem canal de alerta automatizado nesse
projeto) — só logam ERROR + marcam uma tag no MLflow, visível pra quem for
auditar antes de confiar no score desta execução.

Uso: python -m models.anomaly_detection [--contamination auto]
"""

import argparse
import logging
import os

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
import trino
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

from src import trino_io
from src.mlflow_utils import configure_mlflow

load_dotenv()

logger = logging.getLogger(__name__)

ARTIFACT_PATH = os.environ.get(
    "ANOMALY_MODEL_PATH",
    os.path.join(os.path.dirname(__file__), "artifacts", "isolation_forest_contratos.joblib"),
)

MLFLOW_EXPERIMENT = "anomaly_detection"

# Tabela própria do score, schema `ml` (não `gold` — separado do modelo
# dimensional desde 26/07/2026) — não é gravada em `fato_contrato`
# diretamente: `fato_contrato` é `materialized='table'` no dbt (recriada do
# zero a cada `dbt build`), então um UPDATE nela seria apagado no build
# seguinte. `fato_contrato.sql` faz LEFT JOIN nesta tabela (ver
# dbt/models/sources.yml, source `ml_scores`), então o score aparece
# automaticamente no próximo build.
SCORE_TABLE = "iceberg.ml.score_anomalia_contrato"
MODEL_VERSION = "isolation_forest_v2_400trees"

# 400, não 200 — ver "Hiperparâmetros" no topo do módulo: evidência real
# (~216 mil contratos de produção) de que 200 árvores só reproduzem 87,6% do
# "top 1% mais atípico" entre execuções com seeds diferentes; 400 sobe pra
# 92,6%, um ganho real de estabilidade pra quem prioriza revisão manual.
N_ESTIMATORS = 400

# Categorias mais frequentes de tipo_objeto mantidas isoladas; o resto vira "OUTROS"
# (evita one-hot explodir com centenas de descrições de objeto quase únicas).
TOP_TIPO_OBJETO = 8

FEATURE_QUERY = """
SELECT
    f.id_contrato_origem,
    f.ano,
    f.valor_contrato,
    f.flag_emergency,
    dm.descricao_modalidade AS modalidade,
    sc.tipo_objeto,
    sc.data_inicio,
    sc.data_termino,
    COALESCE(dc.historico_infringement, false) AS historico_credor_infringement
FROM iceberg.gold.fato_contrato f
LEFT JOIN iceberg.gold.dim_credor dc ON f.sk_credor = dc.sk_credor
LEFT JOIN iceberg.gold.dim_modalidade dm ON f.sk_modalidade = dm.sk_modalidade
LEFT JOIN iceberg.silver.contratos sc ON CAST(sc.id AS VARCHAR) = f.id_contrato_origem
WHERE f.valor_contrato IS NOT NULL
"""


def connect() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", 8080)),
        user=os.environ.get("TRINO_USER", "notebook"),
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        schema="gold",
    )


def extract_features(conn: trino.dbapi.Connection | None = None) -> pd.DataFrame:
    """Lê a Gold (fato_contrato + dimensões) e a Silver (tipo_objeto/vigência) via Trino."""
    own_conn = conn is None
    conn = conn or connect()
    try:
        cur = conn.cursor()
        cur.execute(FEATURE_QUERY)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    finally:
        if own_conn:
            conn.close()
    return pd.DataFrame(rows, columns=cols)


def build_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Engenharia de features: dias_vigencia, one-hot de modalidade/tipo_objeto, flags booleanas.

    `df` é o resultado bruto de `extract_features` (ou um DataFrame com as mesmas
    colunas, ex: em teste). Retorna só colunas numéricas/binárias, prontas pro
    `IsolationForest` — que não aceita `NaN` nem categórica direto.
    """
    out = pd.DataFrame(index=df.index)
    out["valor_contrato"] = pd.to_numeric(df["valor_contrato"], errors="coerce")

    inicio = pd.to_datetime(df["data_inicio"], errors="coerce")
    termino = pd.to_datetime(df["data_termino"], errors="coerce")
    out["dias_vigencia"] = (termino - inicio).dt.days

    out["flag_emergency"] = df["flag_emergency"].fillna(False).astype(int)
    out["historico_credor_infringement"] = df["historico_credor_infringement"].fillna(False).astype(int)

    top_tipos = df["tipo_objeto"].value_counts().head(TOP_TIPO_OBJETO).index
    tipo_objeto = df["tipo_objeto"].where(df["tipo_objeto"].isin(top_tipos), "OUTROS")
    out = out.join(pd.get_dummies(df["modalidade"].fillna("DESCONHECIDA"), prefix="modalidade"))
    out = out.join(pd.get_dummies(tipo_objeto.fillna("DESCONHECIDO"), prefix="tipo_objeto"))

    # Imputação simples pela mediana (best-effort) — Isolation Forest não aceita NaN.
    out["valor_contrato"] = out["valor_contrato"].fillna(out["valor_contrato"].median())
    out["dias_vigencia"] = out["dias_vigencia"].fillna(out["dias_vigencia"].median())
    return out


def train_model(X: pd.DataFrame, contamination: float | str = "auto", random_state: int = 42) -> IsolationForest:
    model = IsolationForest(contamination=contamination, random_state=random_state, n_estimators=N_ESTIMATORS)
    model.fit(X)
    return model


def score_anomalia(model: IsolationForest, X: pd.DataFrame) -> pd.Series:
    """Converte o `decision_function` (maior = mais normal) em score de anomalia
    em [0, 1] (maior = mais atípico), via min-max scaling sobre o lote."""
    raw = -model.decision_function(X)
    scaled = MinMaxScaler().fit_transform(raw.reshape(-1, 1)).ravel()
    return pd.Series(scaled, index=X.index, name="score_anomalia")


def flag_anomalia(model: IsolationForest, X: pd.DataFrame) -> pd.Series:
    """Classificação binária do próprio Isolation Forest (`predict() == -1`),
    na taxa de `contamination` configurada no treino — complementar ao score
    contínuo: enquanto `summarize_score_distribution()` deixa o limiar de
    revisão em aberto pra calibrar depois, esta flag é "o que o modelo
    classificaria como anomalia hoje, no `contamination` usado"."""
    return pd.Series(model.predict(X) == -1, index=X.index, name="flag_anomalia")


# Thresholds usuais pra virar "sinalizado para revisão" — não é um corte fixo
# no modelo, só o que reportamos pra quem for decidir o ponto de operação real
# (ver docstring do módulo, seção "Desbalanceamento").
SCORE_THRESHOLDS = (0.70, 0.80, 0.90, 0.95)

# Gate de qualidade (ver docstring do módulo) — mudança máxima aceitável no
# score médio entre esta execução e a anterior antes de logar um alerta.
DRIFT_SCORE_MEDIO_MAX_ACEITAVEL = 0.15


def summarize_score_distribution(resultado: pd.DataFrame) -> dict[str, float]:
    """Percentis do score + contagem/percentual de contratos acima de cada
    `SCORE_THRESHOLDS`. Sem rótulo, não dá pra saber a "proporção real" de
    contratos irregulares — isso aqui é a alternativa: mostrar a distribuição
    de verdade pra quem for calibrar o limiar de revisão, em vez de assumir
    uma taxa de anomalia às cegas (tarefa 28).
    """
    scores = resultado["score_anomalia"]
    total = len(scores)
    summary: dict[str, float] = {
        "score_p50": float(scores.quantile(0.50)),
        "score_p75": float(scores.quantile(0.75)),
        "score_p90": float(scores.quantile(0.90)),
        "score_p95": float(scores.quantile(0.95)),
        "score_p99": float(scores.quantile(0.99)),
    }
    for t in SCORE_THRESHOLDS:
        n_acima = int((scores >= t).sum())
        pct = t * 100
        summary[f"n_contratos_score_ge_{pct:.0f}"] = float(n_acima)
        summary[f"pct_contratos_score_ge_{pct:.0f}"] = float(n_acima / total) if total else 0.0
    return summary


def save_model(model: IsolationForest, feature_columns: list[str], path: str = ARTIFACT_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump({"model": model, "feature_columns": feature_columns}, path)
    logger.info("Modelo salvo em %s", path)


def load_model(path: str = ARTIFACT_PATH) -> tuple[IsolationForest, list[str]]:
    bundle = joblib.load(path)
    return bundle["model"], bundle["feature_columns"]


def write_scores(resultado: pd.DataFrame, model_version: str = MODEL_VERSION) -> None:
    """Grava `(id_contrato_origem, ano, score_anomalia, flag_anomalia)` em `SCORE_TABLE`.

    Substitui a tabela inteira a cada chamada (re-score em lote, não
    incremental) — mesmo padrão de `trino_io.replace_table`.
    """
    payload = resultado[["id_contrato_origem", "ano", "score_anomalia", "flag_anomalia"]].copy()
    payload["model_version"] = model_version
    payload["scored_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {SCORE_TABLE} (
            id_contrato_origem varchar,
            ano integer,
            score_anomalia double,
            flag_anomalia boolean,
            model_version varchar,
            scored_at timestamp
        )
    """
    # `flag_anomalia` (tarefa 25/07, incorporado do script da Fernanda) é
    # coluna nova — CREATE TABLE IF NOT EXISTS acima só cobre ambiente do
    # zero; ambiente com a tabela já criada antes desta mudança precisa do
    # ADD COLUMN explícito (idempotente) antes do replace_table.
    trino_io.execute(f"ALTER TABLE IF EXISTS {SCORE_TABLE} ADD COLUMN IF NOT EXISTS flag_anomalia boolean")

    trino_io.replace_table(
        table=SCORE_TABLE,
        df=payload,
        columns=["id_contrato_origem", "ano", "score_anomalia", "flag_anomalia", "model_version", "scored_at"],
        ddl=ddl,
        casts={"scored_at": "TIMESTAMP"},
    )
    logger.info("Gravados %s scores em %s", len(payload), SCORE_TABLE)


def _score_medio_execucao_anterior() -> float | None:
    """Média do `score_anomalia` gravado em produção ANTES desta execução
    sobrescrever `SCORE_TABLE` — usada pelo gate de deriva (ver docstring do
    módulo). `None` na 1ª execução (tabela ainda não existe) ou se a consulta
    falhar por qualquer motivo — nesse caso o gate simplesmente não roda."""
    try:
        df = trino_io.query(f"SELECT AVG(score_anomalia) AS media FROM {SCORE_TABLE}")
    except Exception:
        return None
    if df.empty or pd.isna(df.iloc[0]["media"]):
        return None
    return float(df.iloc[0]["media"])


def run(contamination: float | str = "auto", persist: bool = True) -> pd.DataFrame:
    """Extrai features da Gold/Silver, treina, escora e (por padrão) grava o
    resultado em `SCORE_TABLE` para `fato_contrato` pegar no próximo `dbt build`.

    Cada chamada é uma run do MLflow (tarefa 29) — params do treino, métricas
    do score e o `.joblib` do modelo como artefato, pro time comparar execuções
    ao longo do tempo (ex: se o score médio mudar muito de um dia pro outro).
    """
    configure_mlflow(MLFLOW_EXPERIMENT)
    # Consultado ANTES de sobrescrever SCORE_TABLE (ver write_scores/
    # replace_table) — é a média que estava valendo em produção até agora,
    # usada pelo gate de deriva logo abaixo.
    score_medio_anterior = _score_medio_execucao_anterior()
    with mlflow.start_run(run_name=f"{MODEL_VERSION}_{pd.Timestamp.utcnow():%Y%m%d_%H%M%S}"):
        raw = extract_features()
        X = build_feature_matrix(raw)

        mlflow.log_params(
            {
                "contamination": contamination,
                "n_estimators": N_ESTIMATORS,
                "random_state": 42,
                "n_contratos_entrada": len(raw),
                "n_features": X.shape[1],
            }
        )
        mlflow.set_tag("model_version", MODEL_VERSION)

        model = train_model(X, contamination=contamination)
        scores = score_anomalia(model, X)
        flags = flag_anomalia(model, X)
        save_model(model, list(X.columns))
        # Além do .joblib (usado por load_model/save_model pra recarregar
        # model+feature_columns juntos), loga no formato nativo do MLflow —
        # habilita Model Registry/serving direto pelo MLflow, sem precisar do
        # nosso bundle. Incorporado do script da Fernanda (mlflow.sklearn.log_model).
        mlflow.sklearn.log_model(model, artifact_path="isolation_forest_model")

        resultado = raw[["id_contrato_origem", "ano"]].copy()
        resultado["score_anomalia"] = scores.values
        resultado["flag_anomalia"] = flags.values

        distribuicao = summarize_score_distribution(resultado)
        mlflow.log_metrics(
            {
                "score_medio": float(resultado["score_anomalia"].mean()),
                "score_maximo": float(resultado["score_anomalia"].max()),
                "n_contratos_escorados": float(len(resultado)),
                "n_contratos_flag_anomalia": float(resultado["flag_anomalia"].sum()),
                "pct_contratos_flag_anomalia": float(resultado["flag_anomalia"].mean()),
                **distribuicao,
            }
        )
        logger.info(
            "Scoring concluído: %s contratos, score médio %.4f, score máximo %.4f, "
            "%s contratos com score >= 0.9 (%.1f%%), %s sinalizados pelo predict() (%.1f%%)",
            len(resultado),
            resultado["score_anomalia"].mean(),
            resultado["score_anomalia"].max(),
            int(distribuicao["n_contratos_score_ge_90"]),
            distribuicao["pct_contratos_score_ge_90"] * 100,
            int(resultado["flag_anomalia"].sum()),
            resultado["flag_anomalia"].mean() * 100,
        )

        # Gate de qualidade 1: distribuição degenerada (ver docstring do módulo).
        if distribuicao["score_p50"] == distribuicao["score_p99"]:
            logger.error(
                "ALERTA DE QUALIDADE: distribuição do score degenerada (p50 == p99 == %.4f) — "
                "o modelo não está discriminando contratos neste treino. Não confiar no score "
                "desta execução.",
                distribuicao["score_p50"],
            )
            mlflow.set_tag("alerta_qualidade", "distribuicao_degenerada")

        # Gate de qualidade 2: deriva grande demais vs. a execução anterior.
        score_medio_novo = float(resultado["score_anomalia"].mean())
        if score_medio_anterior is not None:
            diferenca = abs(score_medio_novo - score_medio_anterior)
            mlflow.log_metrics(
                {
                    "score_medio_execucao_anterior": score_medio_anterior,
                    "score_medio_diferenca_vs_execucao_anterior": diferenca,
                }
            )
            if diferenca > DRIFT_SCORE_MEDIO_MAX_ACEITAVEL:
                logger.error(
                    "ALERTA DE QUALIDADE: score médio mudou %.4f -> %.4f (diferença %.4f, acima do "
                    "limite aceitável %.2f) — possível regressão no pipeline de features ou nos "
                    "dados de entrada. Revisar antes de confiar nesta execução.",
                    score_medio_anterior,
                    score_medio_novo,
                    diferenca,
                    DRIFT_SCORE_MEDIO_MAX_ACEITAVEL,
                )
                mlflow.set_tag("alerta_qualidade", "drift_score_medio")

        if persist:
            write_scores(resultado)
        mlflow.log_artifact(ARTIFACT_PATH)
    return resultado


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Modelo 1 — detecção de anomalias em contratos")
    parser.add_argument(
        "--contamination",
        default="auto",
        help="Proporção esperada de contratos anômalos (ex: 0.05) ou 'auto' (padrão do scikit-learn)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    try:
        contamination = float(args.contamination)
    except ValueError:
        contamination = args.contamination
    resultado = run(contamination=contamination)
    print(resultado.sort_values("score_anomalia", ascending=False).head(20).to_string(index=False))
