"""CSS institucional do painel (verde/amarelo, inspirado no site da CGE)."""

import streamlit as st

_CUSTOM_CSS = """
<style>
/* Barra superior (topo do app) em verde, como no site da CGE */
header[data-testid="stHeader"] {
    background-color: #00693E;
}
header[data-testid="stHeader"] * {
    color: #FFFFFF !important;
}

/* Sidebar em verde, com texto branco (labels, títulos e legendas —
   não aplicado dentro dos campos de seleção, que têm fundo claro) */
section[data-testid="stSidebar"] {
    background-color: #00693E;
}
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] span {
    color: #FFFFFF !important;
}
section[data-testid="stSidebar"] div[data-baseweb="select"] * {
    color: #FFFFFF !important;
}

/* Todos os dropdowns do app (fundo verde do tema): o valor já
   selecionado, visível na caixa fechada, fica branco. A lista de
   opções (que abre com fundo branco) não é afetada, pois é
   renderizada fora desse contêiner. */
div[data-baseweb="select"] * {
    color: #FFFFFF !important;
}

/* Cartões de métrica com borda verde arredondada */
div[data-testid="stMetric"] {
    border: 2px solid #00693E;
    border-radius: 12px;
    padding: 16px;
    background-color: #FFFFFF;
}
</style>
"""


def inject_custom_css() -> None:
    st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)
