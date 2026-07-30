"""Painel Streamlit de negócio — Empenhos CE (anomalias e previsão de pagamentos).

Consome iceberg.gold/ml via Trino. Código dividido por responsabilidade:
config.py (conexão/paleta), db.py (Trino), formatting.py (formatação/risco),
sql_filters.py (filtros SQL da sidebar), ai_report.py (IA generativa),
style.py (CSS) e tabs/ (uma aba por módulo).
"""

import os
from pathlib import Path

from dotenv import load_dotenv

import streamlit as st

# Reaproveita o MESMO .env do projeto principal (mesmas variáveis já usadas em
# toda a extração/ML: TRINO_HOST/PORT/USER/HTTP_SCHEME/CATALOG, OPENAI_API_KEY/
# MODEL — ver .env.example na raiz do repo) em vez de exigir uma chave da
# OpenAI duplicada só para este painel. `streamlit/.env` (se existir) é
# carregado por cima e tem prioridade — útil para rodar/testar este painel
# isolado, sem precisar do restante do projeto.
#
# Precisa rodar ANTES de importar config.py (lê as env vars no import) —
# por isso os imports dos módulos do projeto vêm depois, não no topo do
# arquivo (ver noqa: E402 abaixo).
_ROOT_ENV = Path(__file__).resolve().parent.parent / ".env"
_LOCAL_ENV = Path(__file__).resolve().parent / ".env"
load_dotenv(_ROOT_ENV)
load_dotenv(_LOCAL_ENV, override=True)

from db import run_query  # noqa: E402
from style import inject_custom_css  # noqa: E402
from tabs import anomalias, previsao, resumo_ia, visao_geral  # noqa: E402

# Ícone da aba do navegador — brasão do Governo do Ceará (recortado de
# assets/logo_ceara.png, que tem o brasão + a marca "CEARÁ GOVERNO DO
# ESTADO" por extenso, larga demais pra um ícone quadrado).
BRASAO_PATH = os.path.join(os.path.dirname(__file__), "assets", "brasao_ceara.png")

st.set_page_config(
    page_title="Empenhos CE - Anomalias e Previsões",
    page_icon=BRASAO_PATH if os.path.exists(BRASAO_PATH) else "📊",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_custom_css()


@st.cache_data(ttl=600)
def load_anos():
    df = run_query("SELECT DISTINCT ano FROM iceberg.gold.fato_contrato ORDER BY ano DESC")
    return df["ano"].tolist()


@st.cache_data(ttl=600)
def load_orgaos():
    df = run_query(
        """
        SELECT DISTINCT nome, sigla
        FROM iceberg.gold.dim_orgao
        ORDER BY nome
        """
    )
    return df


anos_disponiveis = load_anos()
orgaos_df = load_orgaos()

# ---------------------------------------------------------------------------
# Sidebar - filtros globais (seleção única, não multiseleção)
# ---------------------------------------------------------------------------
st.sidebar.title("Filtros")

ano_selecionado = st.sidebar.selectbox(
    "Ano",
    options=anos_disponiveis,
    index=0 if anos_disponiveis else None,
)
anos_selecionados = [ano_selecionado] if ano_selecionado is not None else []

TODOS_ORGAOS = "Todos os órgãos"
orgao_selecionado = st.sidebar.selectbox(
    "Órgão",
    options=[TODOS_ORGAOS] + orgaos_df["nome"].tolist(),
)
orgaos_selecionados = [] if orgao_selecionado == TODOS_ORGAOS else [orgao_selecionado]

# Sensibilidade do score de anomalia: percentual fixo (não exposto como
# filtro) — ver models/anomaly_detection.py: o score é reescalado min-max a
# cada novo treino, então um percentual (sempre "os X% mais atípicos do lote
# atual") é mais estável que um valor de corte absoluto, mas não precisa ser
# ajustável pelo usuário do painel.
SCORE_PCT_TOP_PADRAO = 10
df_score_cutoff = run_query(
    f"SELECT approx_percentile(score_anomalia, {1 - SCORE_PCT_TOP_PADRAO / 100}) AS cutoff "
    "FROM iceberg.ml.score_anomalia_contrato"
)
score_threshold = float(df_score_cutoff.iloc[0]["cutoff"]) if not df_score_cutoff.empty else 0.7

LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "logo_ceara.png")

col_titulo_app, col_logo_app = st.columns([4, 1])
with col_titulo_app:
    st.title("Empenhos CE — Anomalias e Previsão de Pagamentos")
with col_logo_app:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)

tab_geral, tab_previsao, tab_anomalias, tab_resumo = st.tabs(
    ["Visão Geral", "Previsão de Pagamentos", "Anomalias em Contratos", "Resumo (IA)"]
)

with tab_geral:
    visao_geral.render(anos_selecionados, orgaos_selecionados)

with tab_previsao:
    ano_ref, trimestre_previsto_sel = previsao.render(anos_disponiveis)

with tab_anomalias:
    anomalias.render(anos_selecionados, orgaos_selecionados, score_threshold)

with tab_resumo:
    resumo_ia.render(score_threshold, ano_ref, trimestre_previsto_sel, anos_selecionados, orgaos_selecionados)
