"""CSS institucional do painel (verde/amarelo, inspirado no site da CGE).

Testids/atributos usados abaixo (stMetric, stTabs, stDataFrame, stExpander,
stButton, stHeader, stSidebar, data-baseweb=*) foram conferidos contra o
bundle da versão do Streamlit instalada (`static/static/js/*.js`), não só
copiados de exemplos genéricos — evita regra "morta" (silenciosamente sem
efeito) por mudança de versão.
"""

import streamlit as st

_CUSTOM_CSS = """
<style>
/* ---------- Base ---------- */
html, body, [class*="css"] {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}

.stApp {
    background-color: #F5F8F6;
}

/* A barra superior (stHeader) é fixa/sobreposta ao conteúdo — precisa de
   padding-top suficiente pra não cobrir o título/logo abaixo dela. Um valor
   pequeno aqui (ex: 1.5rem) deixa a barra verde por cima do topo da página. */
.block-container {
    padding-top: 5rem;
    padding-bottom: 3rem;
}

h1 {
    color: #00693E;
    font-weight: 700;
    letter-spacing: -0.5px;
}

/* Barra superior (topo do app) em verde, como no site da CGE */
header[data-testid="stHeader"] {
    background-color: #00693E;
}
header[data-testid="stHeader"] * {
    color: #FFFFFF !important;
}

/* ---------- Sidebar (verde, texto branco) ---------- */
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
/* Caixa do dropdown em branco com texto escuro — contraste garantido contra
   o fundo verde escuro da sidebar, em vez de tentar forçar texto claro por
   cima do verde (frágil: qualquer elemento interno do baseweb que escape da
   regra vira texto escuro invisível sobre fundo escuro, exatamente o defeito
   relatado). Só dentro da sidebar; a lista de opções, que abre por cima do
   conteúdo principal, não é afetada. */
section[data-testid="stSidebar"] div[data-baseweb="select"] {
    background-color: #FFFFFF;
    border-radius: 8px;
}
section[data-testid="stSidebar"] div[data-baseweb="select"] * {
    color: #16281F !important;
}
section[data-testid="stSidebar"] hr {
    border-color: rgba(255, 255, 255, 0.25);
}

/* Selects FORA da sidebar (área principal, ex: "Ano de referência",
   "Órgão"): o tema do app (.streamlit/config.toml, secondaryBackgroundColor)
   deixa o fundo desses widgets verde escuro em QUALQUER lugar da página, não
   só na sidebar. Em vez de manter o verde e só ajustar o texto, o fundo vira
   amarelo (cor de destaque do branding, mesma do primaryColor/COR_EMPENHADO)
   — dá mais variedade visual ao painel (nem tudo verde) e o texto escuro
   correspondente garante contraste. Regra sem escopo de sidebar, mas com
   especificidade menor que a regra de dentro da sidebar acima — só se aplica
   onde a outra, mais específica, não vale (fora da sidebar). */
div[data-baseweb="select"] {
    background-color: #F5B301;
    border-radius: 8px;
}
div[data-baseweb="select"] * {
    color: #16281F !important;
}

/* ---------- Abas ---------- */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    border-bottom: 2px solid #E3E8E5;
}
.stTabs [data-baseweb="tab"] {
    height: 44px;
    color: #5A6B63;
    font-weight: 500;
}
.stTabs [aria-selected="true"] {
    color: #00693E !important;
    font-weight: 700;
}

/* ---------- Cartões de métrica ---------- */
div[data-testid="stMetric"] {
    background-color: #FFFFFF;
    border: 1px solid #E3E8E5;
    border-left: 4px solid #00693E;
    border-radius: 10px;
    padding: 16px 18px;
    box-shadow: 0 1px 3px rgba(15, 40, 30, 0.06);
    transition: box-shadow 0.15s ease, transform 0.15s ease;
}
div[data-testid="stMetric"]:hover {
    box-shadow: 0 4px 10px rgba(15, 40, 30, 0.10);
    transform: translateY(-1px);
}
div[data-testid="stMetricLabel"] {
    font-size: 0.8rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: #5A6B63;
}
div[data-testid="stMetricValue"] {
    font-weight: 700;
    /* Reduzida suavemente (padrão do Streamlit é maior) — valores formatados
       mais longos (ex: "R$ 107,41 bi") cortavam dentro do cartão. */
    font-size: 1.7rem;
    color: #16281F;
}

/* ---------- Tabelas e gráficos: mesmo tratamento de "cartão" das métricas ---------- */
div[data-testid="stDataFrame"],
div[data-testid="stPlotlyChart"],
div[data-testid="stExpander"] {
    border: 1px solid #E3E8E5;
    border-radius: 10px;
    background-color: #FFFFFF;
}
div[data-testid="stDataFrame"],
div[data-testid="stPlotlyChart"] {
    padding: 6px;
}

/* ---------- Botões ---------- */
div[data-testid="stButton"] button[kind="primary"] {
    background-color: #00693E;
    border-color: #00693E;
    color: #FFFFFF !important;
}
div[data-testid="stButton"] button[kind="primary"]:hover {
    background-color: #00522F;
    border-color: #00522F;
    color: #FFFFFF !important;
}

/* ---------- Divisores ---------- */
hr {
    margin: 1.25rem 0;
    border-color: #E3E8E5;
}
</style>
"""


def inject_custom_css() -> None:
    st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)
