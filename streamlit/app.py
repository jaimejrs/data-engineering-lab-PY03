import decimal
import os
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import trino
from dotenv import load_dotenv
from openai import OpenAI

import streamlit as st

_ROOT_ENV = Path(__file__).resolve().parent.parent / ".env"
_LOCAL_ENV = Path(__file__).resolve().parent / ".env"
load_dotenv(_ROOT_ENV)
load_dotenv(_LOCAL_ENV, override=True)

# ---------------------------------------------------------------------------
# Configuração de conexão
# ---------------------------------------------------------------------------
HOST = os.getenv("TRINO_HOST", "100.69.31.14")
PORT = int(os.getenv("TRINO_PORT", "8085"))
USER = os.getenv("TRINO_USER", "arthur")
HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "http")
CATALOG = os.getenv("TRINO_CATALOG", "iceberg")

st.set_page_config(
    page_title="Empenhos CE - Anomalias e Previsões",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
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
    """,
    unsafe_allow_html=True,
)


def get_connection():
    return trino.dbapi.connect(
        host=HOST,
        port=PORT,
        user=USER,
        catalog=CATALOG,
        schema="gold",
        http_scheme=HTTP_SCHEME,
    )


@st.cache_data(ttl=300, show_spinner="Consultando o Trino...")
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    # O cliente do Trino retorna colunas DECIMAL como decimal.Decimal.
    # Convertemos para float para evitar erros ao misturar com float/NaN
    # em operações posteriores (merge, fillna, subtração, etc).
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
            df[col] = df[col].astype(float)

    return df


def formatar_bilhoes(valor: float) -> str:
    """Formata um valor em R$ na casa de bilhões, ex: R$ 1,23 bi."""
    bi = (valor or 0) / 1_000_000_000
    texto = f"{bi:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto} bi"


# Paleta institucional
COR_PAGO = "#1B8A3D"
COR_EMPENHADO = "#F5B301"
COR_PREVISTO = "#E74C3C"

FAIXA_BAIXO = "Baixo (< 0,70)"
FAIXA_MEDIO = "Médio (0,70 a 0,85)"
FAIXA_ALTO = "Alto (> 0,85)"
CORES_FAIXA_RISCO = {
    FAIXA_BAIXO: "#2ecc71",
    FAIXA_MEDIO: "#f1c40f",
    FAIXA_ALTO: "#e74c3c",
}


def classificar_risco(score: float) -> str:
    if score < 0.70:
        return FAIXA_BAIXO
    elif score <= 0.85:
        return FAIXA_MEDIO
    else:
        return FAIXA_ALTO


def _fmt_reais(valor) -> str:
    if valor is None or pd.isna(valor):
        return "não informado"
    texto = f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto}"


def escapar_cifrao_markdown(texto: str) -> str:
    """
    O st.markdown interpreta pares de '$' como delimitadores de fórmula
    LaTeX (KaTeX). Como o relatório tem vários "R$" no texto, isso faz
    trechos inteiros virarem "fórmulas" quebradas visualmente. Escapamos
    o cifrão para que apareça como texto literal.
    """
    return texto.replace("$", "\\$")


def build_prompt(anomalias: pd.DataFrame, previsoes: pd.DataFrame) -> tuple[str, str]:
    """Monta o prompt (system + user) com os números já calculados pelos dois
    modelos, em texto — o LLM só reescreve/traduz, não recebe dado bruto nem
    infere números novos (evita alucinação de valores)."""
    system_prompt = (
        "Você escreve relatórios executivos curtos para gestores públicos do "
        "Estado do Ceará, sem formação técnica em dados ou estatística. "
        "Use português claro, direto, sem jargão técnico (não use termos como "
        "'score', 'quantil', 'XGBoost', 'Isolation Forest', 'p10/p50/p90', "
        "'feature' — traduza tudo para linguagem de gestão pública). Não "
        "invente números: use apenas os valores fornecidos. Formate em "
        "Markdown com títulos de seção."
    )

    if anomalias.empty:
        anomalias_txt = "Nenhum contrato com indício de comportamento atípico foi identificado nesta execução."
    else:
        linhas = []
        for _, r in anomalias.iterrows():
            emergencial = " (contratação emergencial)" if r.get("flag_emergency") else ""
            linhas.append(
                f"- Contrato {r['id_contrato_origem']} ({int(r['ano'])}), órgão: {r['orgao'] or 'não identificado'}, "
                f"credor: {r['credor'] or 'não identificado'}, modalidade: {r['modalidade'] or 'não informada'}, "
                f"valor: {_fmt_reais(r['valor_contrato'])}, grau de atipicidade: {float(r['score_anomalia']):.0%}"
                f"{emergencial}"
            )
        anomalias_txt = "\n".join(linhas)

    if previsoes.empty:
        previsoes_txt = "Nenhuma previsão de pagamento disponível nesta execução."
    else:
        linhas = []
        for _, r in previsoes.iterrows():
            linhas.append(
                f"- {r['nome_orgao'] or r['codigo_orgao']}, {int(r['trimestre_previsto'])}º trimestre de "
                f"{int(r['ano_previsto'])}: previsão central {_fmt_reais(r['valor_previsto_p50'])} "
                f"(intervalo entre {_fmt_reais(r['valor_previsto_p10'])} e {_fmt_reais(r['valor_previsto_p90'])})"
            )
        previsoes_txt = "\n".join(linhas)

    user_prompt = f"""Escreva um relatório narrativo com os resultados de dois modelos analíticos
rodados sobre os contratos e pagamentos públicos do Estado do Ceará.

## Contratos com maior grau de atipicidade (modelo de detecção de anomalias)
{anomalias_txt}

## Previsão de pagamentos por órgão para o próximo trimestre
{previsoes_txt}

Estruture o relatório em 4 seções, com um título Markdown para cada:
1. "Visão geral" — 2-3 frases resumindo o que este relatório cobre.
2. "Contratos que merecem atenção" — explique por que os contratos listados
   chamaram atenção do modelo (valor fora do padrão, modalidade, credor com
   histórico, contratação emergencial etc.), em linguagem acessível. Deixe
   claro que "atipicidade" não significa irregularidade comprovada — é um
   sinal para revisão por um analista humano, não uma acusação.
3. "Previsão de pagamentos" — explique o que o gestor deve esperar de
   desembolso por órgão no próximo trimestre e o que o intervalo (menor e
   maior valor possível) significa na prática.
4. "Recomendações" — 2-4 ações práticas e específicas para o gestor,
   baseadas apenas no que foi listado acima.
"""
    return system_prompt, user_prompt


@st.cache_resource(show_spinner=False)
def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY não encontrada. Defina essa variável no arquivo .env "
            "na raiz do projeto (veja .env.example)."
        )
    return OpenAI(api_key=api_key)


def gerar_relatorio_ia(anomalias: pd.DataFrame, previsoes: pd.DataFrame) -> str:
    system_prompt, user_prompt = build_prompt(anomalias, previsoes)
    client = get_openai_client()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    resposta = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
    )
    return resposta.choices[0].message.content


# ---------------------------------------------------------------------------
# Carregamento de opções de filtro
# ---------------------------------------------------------------------------
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
# Sidebar - filtros globais
# ---------------------------------------------------------------------------
st.sidebar.title("Filtros")

anos_selecionados = st.sidebar.multiselect(
    "Ano",
    options=anos_disponiveis,
    default=anos_disponiveis[:1] if anos_disponiveis else [],
)

orgaos_selecionados = st.sidebar.multiselect(
    "Órgão (opcional)",
    options=orgaos_df["nome"].tolist(),
)

st.sidebar.divider()
score_threshold = st.sidebar.slider(
    "Score mínimo de anomalia",
    min_value=0.0,
    max_value=1.0,
    value=0.7,
    step=0.05,
    help="Contratos com score_anomalia acima deste valor são considerados na aba de Anomalias.",
)


def _anos_filter_sql(column: str) -> str:
    if not anos_selecionados:
        return ""
    anos_str = ", ".join(str(a) for a in anos_selecionados)
    return f"AND {column} IN ({anos_str})"


def _orgaos_filter_sql(column: str) -> str:
    if not orgaos_selecionados:
        return ""
    nomes = ", ".join("'" + o.replace("'", "''") + "'" for o in orgaos_selecionados)
    return f"AND {column} IN ({nomes})"


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

# ---------------------------------------------------------------------------
# ABA 0 — Visão Geral
# ---------------------------------------------------------------------------
with tab_geral:
    query_kpis_geral = f"""
        SELECT
            SUM(fc.valor_empenhado) AS total_empenhado,
            SUM(fc.valor_pago) AS total_pago
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {_anos_filter_sql("fc.ano")}
        {_orgaos_filter_sql("dorg.nome")}
    """
    df_kpis_geral = run_query(query_kpis_geral)

    total_empenhado = df_kpis_geral.iloc[0]["total_empenhado"] if not df_kpis_geral.empty else 0
    total_pago = df_kpis_geral.iloc[0]["total_pago"] if not df_kpis_geral.empty else 0
    total_empenhado = total_empenhado or 0
    total_pago = total_pago or 0

    aproveitamento_pct = (total_pago / total_empenhado * 100) if total_empenhado else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Valor total empenhado", formatar_bilhoes(total_empenhado))
    col2.metric("Valor total pago", formatar_bilhoes(total_pago))
    col3.metric(
        "Aproveitamento do empenhado",
        f"{aproveitamento_pct:.1f}%",
        help="% do valor empenhado que já foi efetivamente pago (valor pago / valor empenhado).",
    )

    st.divider()

    st.subheader("Pagamentos vs. Empenhado ao longo do ano")

    query_mensal = f"""
        SELECT
            dt.mes,
            SUM(fc.valor_pago) AS valor_pago,
            SUM(fc.valor_empenhado) AS valor_empenhado
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_tempo dt ON fc.sk_tempo = dt.sk_tempo
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {_anos_filter_sql("fc.ano")}
        {_orgaos_filter_sql("dorg.nome")}
        GROUP BY dt.mes
        ORDER BY dt.mes
    """
    df_mensal = run_query(query_mensal)

    if df_mensal.empty:
        st.info("Nenhum dado encontrado para os filtros atuais.")
    else:
        df_mensal_long = df_mensal.melt(
            id_vars="mes",
            value_vars=["valor_pago", "valor_empenhado"],
            var_name="tipo",
            value_name="valor",
        )
        df_mensal_long["tipo"] = df_mensal_long["tipo"].map({"valor_pago": "Pago", "valor_empenhado": "Empenhado"})

        fig_mensal = px.line(
            df_mensal_long,
            x="mes",
            y="valor",
            color="tipo",
            markers=True,
            color_discrete_map={"Pago": COR_PAGO, "Empenhado": COR_EMPENHADO},
            title="Pago vs. Empenhado por mês",
            labels={"valor": "Valor (R$)", "mes": "Mês", "tipo": "Origem"},
        )
        fig_mensal.update_traces(line=dict(width=3), marker=dict(size=8))
        fig_mensal.update_xaxes(dtick=1)
        st.plotly_chart(fig_mensal, use_container_width=True)

    st.divider()

    query_aproveitamento = f"""
        SELECT
            dorg.nome AS nome_orgao,
            dorg.sigla AS sigla_orgao,
            SUM(fc.valor_empenhado) AS total_empenhado,
            SUM(fc.valor_pago) AS total_pago
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {_anos_filter_sql("fc.ano")}
        {_orgaos_filter_sql("dorg.nome")}
        GROUP BY dorg.nome, dorg.sigla
    """
    df_aproveitamento = run_query(query_aproveitamento)

    if df_aproveitamento.empty:
        st.info("Nenhum órgão encontrado para os filtros atuais.")
    else:
        df_aproveitamento = df_aproveitamento[df_aproveitamento["total_empenhado"] > 100_000_000].copy()

        if df_aproveitamento.empty:
            st.info("Nenhum órgão com mais de R$ 100 milhões empenhados no ano para os filtros atuais.")
        else:
            df_aproveitamento["aproveitamento_pct"] = (
                df_aproveitamento["total_pago"] / df_aproveitamento["total_empenhado"] * 100
            )
            top5_aproveitamento = df_aproveitamento.sort_values("aproveitamento_pct", ascending=False).head(5)

            col_titulo, col_select = st.columns([2, 1])

            with col_select:
                orgao_escolhido = st.selectbox(
                    "Órgão (top 5 por aproveitamento)",
                    options=top5_aproveitamento["nome_orgao"].tolist(),
                    key="orgao_aproveitamento_selecionado",
                )

            with col_titulo:
                st.subheader(orgao_escolhido)

            linha_orgao = top5_aproveitamento[top5_aproveitamento["nome_orgao"] == orgao_escolhido].iloc[0]

            nome_escapado = orgao_escolhido.replace("'", "''")
            query_evolucao_orgao = f"""
                SELECT
                    dt.mes,
                    SUM(fc.valor_empenhado) AS valor_empenhado,
                    SUM(fc.valor_pago) AS valor_pago
                FROM iceberg.gold.fato_contrato fc
                JOIN iceberg.gold.dim_tempo dt ON fc.sk_tempo = dt.sk_tempo
                JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
                WHERE dorg.nome = '{nome_escapado}'
                {_anos_filter_sql("fc.ano")}
                GROUP BY dt.mes
                ORDER BY dt.mes
            """
            df_evolucao_orgao = run_query(query_evolucao_orgao)

            col_grafico, col_pct = st.columns([3, 1])

            with col_grafico:
                if df_evolucao_orgao.empty:
                    st.info("Nenhum dado mensal encontrado para este órgão.")
                else:
                    df_evolucao_long = df_evolucao_orgao.melt(
                        id_vars="mes",
                        value_vars=["valor_empenhado", "valor_pago"],
                        var_name="tipo",
                        value_name="valor",
                    )
                    df_evolucao_long["tipo"] = df_evolucao_long["tipo"].map(
                        {"valor_empenhado": "Empenhado", "valor_pago": "Pago"}
                    )

                    fig_evolucao = px.line(
                        df_evolucao_long,
                        x="mes",
                        y="valor",
                        color="tipo",
                        markers=True,
                        color_discrete_map={"Empenhado": COR_EMPENHADO, "Pago": COR_PAGO},
                        title=f"Empenhado vs. Pago por mês — {orgao_escolhido}",
                        labels={"valor": "Valor (R$)", "mes": "Mês", "tipo": "Origem"},
                    )
                    fig_evolucao.update_traces(line=dict(width=3), marker=dict(size=8))
                    fig_evolucao.update_xaxes(dtick=1)
                    st.plotly_chart(fig_evolucao, use_container_width=True)

            with col_pct:
                st.metric(
                    "Aproveitamento do empenho",
                    f"{linha_orgao['aproveitamento_pct']:.1f}%",
                    help="% do valor empenhado por este órgão que já foi efetivamente pago.",
                )
                st.metric("Total empenhado", formatar_bilhoes(linha_orgao["total_empenhado"]))
                st.metric("Total pago", formatar_bilhoes(linha_orgao["total_pago"]))

# ---------------------------------------------------------------------------
# ABA 1 — Anomalias em Contratos
# ---------------------------------------------------------------------------
with tab_anomalias:
    query_anomalias = f"""
        SELECT
            fc.id_contrato_origem,
            fc.ano,
            fc.num_spu,
            fc.valor_contrato,
            fc.valor_empenhado,
            fc.valor_pago,
            fc.status,
            fc.flag_emergency,
            fc.score_anomalia,
            sac.flag_anomalia,
            sac.model_version,
            dorg.nome AS nome_orgao,
            dorg.sigla AS sigla_orgao,
            dorg.nome_municipio,
            dcred.nome AS nome_credor,
            dcred.cnpj_cpf,
            dcred.tipo AS tipo_credor,
            dcred.historico_infringement,
            dmod.descricao_modalidade
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        JOIN iceberg.gold.dim_credor dcred
            ON fc.sk_credor = dcred.sk_credor AND dcred.versao_atual = true
        JOIN iceberg.gold.dim_modalidade dmod ON fc.sk_modalidade = dmod.sk_modalidade
        LEFT JOIN iceberg.ml.score_anomalia_contrato sac
            ON fc.id_contrato_origem = sac.id_contrato_origem AND fc.ano = sac.ano
        WHERE fc.score_anomalia >= {score_threshold}
        {_anos_filter_sql("fc.ano")}
        {_orgaos_filter_sql("dorg.nome")}
        ORDER BY fc.score_anomalia DESC
    """

    query_valor_total_contratos = f"""
        SELECT SUM(fc.valor_contrato) AS valor_total
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {_anos_filter_sql("fc.ano")}
        {_orgaos_filter_sql("dorg.nome")}
    """

    df_anom = run_query(query_anomalias)
    df_valor_total = run_query(query_valor_total_contratos)
    valor_total_contratos = (
        df_valor_total.iloc[0]["valor_total"]
        if not df_valor_total.empty and pd.notna(df_valor_total.iloc[0]["valor_total"])
        else 0
    )

    if df_anom.empty:
        st.info("Nenhum contrato encontrado com os filtros atuais. Tente reduzir o score mínimo.")
    else:
        df_anom["faixa_risco"] = df_anom["score_anomalia"].astype(float).apply(classificar_risco)
        df_medio_alto = df_anom[df_anom["faixa_risco"].isin([FAIXA_MEDIO, FAIXA_ALTO])]

        valor_anomalos = df_medio_alto["valor_contrato"].sum()
        pct_valor_anomalo = (valor_anomalos / valor_total_contratos * 100) if valor_total_contratos else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric(
            "Contratos anômalos (médio + alto risco)",
            f"{len(df_medio_alto):,}".replace(",", "."),
            help="Contratos classificados como risco médio (0,70 a 0,85) ou alto (> 0,85).",
        )
        col2.metric(
            "Valor total (contratos)",
            formatar_bilhoes(valor_total_contratos),
            help="Valor de todos os contratos do período filtrado (ano/órgão), independente do score de anomalia.",
        )
        col3.metric(
            "Valor contratos anômalos",
            formatar_bilhoes(valor_anomalos),
            help="Soma do valor dos contratos classificados como risco médio ou alto.",
        )
        col4.metric(
            "% do valor total (anômalos)",
            f"{pct_valor_anomalo:.1f}%",
            help="Percentual que o valor dos contratos médio+alto risco representa do valor total de contratos do período.",
        )

        col5, col6 = st.columns(2)
        col5.metric("Órgãos afetados (médio + alto risco)", df_medio_alto["nome_orgao"].nunique())
        col6.metric(
            "Score médio (médio + alto risco)",
            f"{df_medio_alto['score_anomalia'].mean():.3f}" if not df_medio_alto.empty else "N/D",
        )

        st.divider()

        colA, colB = st.columns([1, 1])

        with colA:
            top_orgaos = (
                df_medio_alto.groupby("nome_orgao")
                .size()
                .reset_index(name="qtd_contratos")
                .sort_values("qtd_contratos", ascending=False)
                .head(10)
            )
            fig_top_orgaos = px.bar(
                top_orgaos,
                x="qtd_contratos",
                y="nome_orgao",
                orientation="h",
                title="Top 10 órgãos por nº de contratos anômalos",
                labels={"qtd_contratos": "Contratos", "nome_orgao": "Órgão"},
                color_discrete_sequence=[COR_PAGO],
            )
            fig_top_orgaos.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_top_orgaos, use_container_width=True)

        with colB:
            bin_edges = np.arange(0, 1.05, 0.05)
            contagens, edges = np.histogram(df_anom["score_anomalia"].astype(float), bins=bin_edges)
            centros = (edges[:-1] + edges[1:]) / 2

            df_hist = pd.DataFrame(
                {
                    "centro": centros,
                    "contratos": contagens,
                    "faixa": [classificar_risco(c) for c in centros],
                }
            )

            fig_hist = px.bar(
                df_hist,
                x="centro",
                y="contratos",
                color="faixa",
                color_discrete_map=CORES_FAIXA_RISCO,
                category_orders={"faixa": [FAIXA_BAIXO, FAIXA_MEDIO, FAIXA_ALTO]},
                title="Distribuição do score de anomalia",
                labels={"centro": "Score de anomalia", "contratos": "Nº de contratos", "faixa": "Risco"},
            )
            fig_hist.update_traces(marker_line_width=0)
            fig_hist.update_layout(bargap=0.02)
            st.plotly_chart(fig_hist, use_container_width=True)

        st.subheader("Contratos com maior score de anomalia")
        st.dataframe(
            df_anom[
                [
                    "id_contrato_origem",
                    "ano",
                    "nome_orgao",
                    "nome_credor",
                    "tipo_credor",
                    "historico_infringement",
                    "descricao_modalidade",
                    "valor_contrato",
                    "valor_pago",
                    "score_anomalia",
                    "flag_anomalia",
                    "status",
                    "flag_emergency",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

# ---------------------------------------------------------------------------
# ABA 2 — Pago por trimestre (trimestre selecionado usa a previsão)
# ---------------------------------------------------------------------------
with tab_previsao:
    st.subheader("Valor pago por trimestre")

    col_ano, col_tri = st.columns(2)
    ano_ref = col_ano.selectbox(
        "Ano de referência",
        options=sorted(set(anos_disponiveis) | {2026}, reverse=True),
        index=0,
    )
    trimestre_previsto_sel = col_tri.selectbox(
        "Trimestre a substituir pela previsão",
        options=[1, 2, 3],
        index=2,  # padrão: 3º trimestre
    )

    st.caption(
        "Soma total de todos os órgãos por trimestre. Os trimestres com dado real usam "
        "o valor efetivamente pago; o trimestre selecionado acima usa a previsão do "
        "modelo (mediana, com intervalo P10-P90)."
    )

    query_empenho_trimestre = f"""
        SELECT dt.trimestre, SUM(fc.valor_pago) AS valor_pago_trimestre
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_tempo dt ON fc.sk_tempo = dt.sk_tempo
        WHERE fc.ano = {ano_ref}
        GROUP BY dt.trimestre
        ORDER BY dt.trimestre
    """

    query_previsto_trimestre = f"""
        SELECT SUM(valor_previsto_p50) AS p50
        FROM iceberg.ml.previsao_pagamento_orgao
        WHERE ano_previsto = {ano_ref}
          AND trimestre_previsto = {trimestre_previsto_sel}
    """

    df_emp_tri = run_query(query_empenho_trimestre)
    df_prev_tri = run_query(query_previsto_trimestre)

    real_map = (
        dict(zip(df_emp_tri["trimestre"], df_emp_tri["valor_pago_trimestre"], strict=False))
        if not df_emp_tri.empty
        else {}
    )

    prev_p50 = None
    if not df_prev_tri.empty and pd.notna(df_prev_tri.iloc[0]["p50"]):
        prev_p50 = df_prev_tri.iloc[0]["p50"]

    trimestres = [1, 2, 3]
    valores, tipos = [], []
    for t in trimestres:
        if t == trimestre_previsto_sel and prev_p50 is not None:
            valores.append(prev_p50)
            tipos.append("Previsto")
        else:
            valores.append(real_map.get(t, 0))
            tipos.append("Real (pago)")

    df_chart = pd.DataFrame(
        {
            "trimestre": [f"T{t}" for t in trimestres],
            "valor": valores,
            "tipo": tipos,
        }
    )

    total_ano = sum(valores)
    valor_trimestre_sel = valores[trimestre_previsto_sel - 1]

    col1, col2 = st.columns(2)
    col1.metric(
        f"Total {ano_ref} (real + previsto)",
        formatar_bilhoes(total_ano),
    )
    col2.metric(
        f"Valor em {ano_ref}-T{trimestre_previsto_sel} ({tipos[trimestre_previsto_sel - 1].lower()})",
        formatar_bilhoes(valor_trimestre_sel),
    )

    st.divider()

    query_top5 = f"""
        SELECT
            nome_orgao,
            valor_previsto_p10,
            valor_previsto_p50,
            valor_previsto_p90
        FROM iceberg.ml.previsao_pagamento_orgao
        WHERE ano_previsto = {ano_ref}
          AND trimestre_previsto = {trimestre_previsto_sel}
        ORDER BY valor_previsto_p50 DESC
        LIMIT 5
    """
    df_top5 = run_query(query_top5)

    if not df_top5.empty:
        df_top5["erro_mais"] = df_top5["valor_previsto_p90"] - df_top5["valor_previsto_p50"]
        df_top5["erro_menos"] = df_top5["valor_previsto_p50"] - df_top5["valor_previsto_p10"]

    col_esq, col_dir = st.columns(2)

    with col_esq:
        fig_tri = px.bar(
            df_chart,
            x="trimestre",
            y="valor",
            color="tipo",
            color_discrete_map={"Real (pago)": COR_PAGO, "Previsto": COR_PREVISTO},
            title=f"Valor pago por trimestre — {ano_ref}",
            labels={"valor": "Valor (R$)", "trimestre": "Trimestre", "tipo": "Origem"},
        )
        st.plotly_chart(fig_tri, use_container_width=True)

    with col_dir:
        if df_top5.empty:
            st.info("Nenhuma previsão encontrada para o ano/trimestre selecionados.")
        else:
            fig_top5 = px.bar(
                df_top5,
                x="nome_orgao",
                y="valor_previsto_p50",
                error_y="erro_mais",
                error_y_minus="erro_menos",
                title=f"Top 5 pagamentos previstos — {ano_ref}-T{trimestre_previsto_sel} (erro = P10-P90)",
                labels={"valor_previsto_p50": "Valor previsto (mediana, R$)", "nome_orgao": "Órgão"},
                color_discrete_sequence=[COR_PAGO],
            )
            fig_top5.update_layout(xaxis_tickangle=-30)
            st.plotly_chart(fig_top5, use_container_width=True)

    st.subheader("Dados detalhados")
    col_esq2, col_dir2 = st.columns(2)
    with col_esq2:
        st.dataframe(
            df_chart[["trimestre", "tipo", "valor"]],
            use_container_width=True,
            hide_index=True,
        )
    with col_dir2:
        if not df_top5.empty:
            st.dataframe(
                df_top5[["nome_orgao", "valor_previsto_p10", "valor_previsto_p50", "valor_previsto_p90"]],
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    st.subheader(f"Pago vs. Previsto — todos os trimestres de {ano_ref}")
    st.caption(
        "Diferente do gráfico acima (que substitui um trimestre pela previsão), "
        "aqui os dois valores aparecem lado a lado para cada trimestre disponível na tabela de previsão."
    )

    query_previsto_todos = f"""
        SELECT trimestre_previsto, SUM(valor_previsto_p50) AS p50
        FROM iceberg.ml.previsao_pagamento_orgao
        WHERE ano_previsto = {ano_ref}
        GROUP BY trimestre_previsto
        ORDER BY trimestre_previsto
    """
    df_previsto_todos = run_query(query_previsto_todos)
    previsto_map = (
        dict(zip(df_previsto_todos["trimestre_previsto"], df_previsto_todos["p50"], strict=False))
        if not df_previsto_todos.empty
        else {}
    )

    trimestres_disponiveis = sorted(set(real_map.keys()) | set(previsto_map.keys()))
    if not trimestres_disponiveis:
        st.info(f"Nenhum dado de pagamento ou previsão encontrado para {ano_ref}.")
    else:
        df_full = pd.DataFrame(
            {
                "trimestre": [f"T{t}" for t in trimestres_disponiveis for _ in range(2)],
                "valor": [v for t in trimestres_disponiveis for v in (real_map.get(t, 0), previsto_map.get(t, 0))],
                "tipo": ["Pago", "Previsto"] * len(trimestres_disponiveis),
            }
        )

        fig_full = px.line(
            df_full,
            x="trimestre",
            y="valor",
            color="tipo",
            markers=True,
            color_discrete_map={"Pago": COR_PAGO, "Previsto": COR_PREVISTO},
            title=f"Pago vs. Previsto (mediana) por trimestre — {ano_ref}",
            labels={"valor": "Valor (R$)", "trimestre": "Trimestre", "tipo": "Origem"},
        )
        fig_full.update_traces(line=dict(width=3), marker=dict(size=9))
        st.plotly_chart(fig_full, use_container_width=True)

        st.dataframe(
            df_full.pivot(index="trimestre", columns="tipo", values="valor").reset_index(),
            use_container_width=True,
            hide_index=True,
        )

# ---------------------------------------------------------------------------
# ABA 3 — Resumo narrativo gerado por IA (sob demanda)
# ---------------------------------------------------------------------------
with tab_resumo:
    st.subheader("Relatório narrativo (gerado por IA)")
    st.caption(
        "Ao clicar no botão, os contratos com maior grau de atipicidade e as "
        "previsões de pagamento mais recentes são enviados como texto já "
        "calculado para o modelo de linguagem, que apenas reescreve os "
        "achados em português claro — nenhum valor é inventado pela IA."
    )

    if st.button("🪄 Gerar novo relatório com IA", type="primary"):
        query_anomalias_ia = f"""
            SELECT
                fc.id_contrato_origem,
                fc.ano,
                fc.valor_contrato,
                fc.flag_emergency,
                fc.score_anomalia,
                dorg.nome AS orgao,
                dcred.nome AS credor,
                dmod.descricao_modalidade AS modalidade
            FROM iceberg.gold.fato_contrato fc
            JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
            JOIN iceberg.gold.dim_credor dcred
                ON fc.sk_credor = dcred.sk_credor AND dcred.versao_atual = true
            JOIN iceberg.gold.dim_modalidade dmod ON fc.sk_modalidade = dmod.sk_modalidade
            WHERE fc.score_anomalia >= {score_threshold}
            ORDER BY fc.score_anomalia DESC
            LIMIT 15
        """
        query_previsoes_ia = f"""
            SELECT codigo_orgao, nome_orgao, ano_previsto, trimestre_previsto,
                   valor_previsto_p10, valor_previsto_p50, valor_previsto_p90
            FROM iceberg.ml.previsao_pagamento_orgao
            WHERE ano_previsto = {ano_ref}
              AND trimestre_previsto = {trimestre_previsto_sel}
            ORDER BY valor_previsto_p50 DESC
        """

        with st.spinner("Consultando dados e gerando relatório..."):
            df_anom_ia = run_query(query_anomalias_ia)
            df_prev_ia = run_query(query_previsoes_ia)
            try:
                st.session_state["relatorio_ia_texto"] = gerar_relatorio_ia(df_anom_ia, df_prev_ia)
                st.session_state["relatorio_ia_meta"] = (
                    f"{len(df_anom_ia)} contratos anômalos · {df_prev_ia['nome_orgao'].nunique() if not df_prev_ia.empty else 0} "
                    f"órgãos previstos · referência {ano_ref}-T{trimestre_previsto_sel}"
                )
            except Exception as e:
                st.error(f"Erro ao gerar relatório: {e}")

    if "relatorio_ia_texto" in st.session_state:
        st.divider()
        st.caption(st.session_state.get("relatorio_ia_meta", ""))
        st.markdown(escapar_cifrao_markdown(st.session_state["relatorio_ia_texto"]))
