"""Aba "Visão Geral" — KPIs agregados, série mensal e top-5 por aproveitamento."""

import plotly.express as px
from config import COR_EMPENHADO, COR_PAGO
from db import run_query
from formatting import formatar_bilhoes, formatar_moeda_adaptativo
from sql_filters import anos_filter_sql, orgaos_filter_sql

import streamlit as st

CORTE_TOP5_APROVEITAMENTO = 100_000_000


def render(anos_selecionados: list, orgaos_selecionados: list) -> None:
    query_kpis_geral = f"""
        SELECT
            SUM(fc.valor_empenhado) AS total_empenhado,
            SUM(fc.valor_pago) AS total_pago
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
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
        help=(
            "% do valor empenhado que já foi efetivamente pago (valor pago / valor empenhado). "
            "Acima de 100% é possível: acontece quando o valor pago já inclui aditivo ou ajuste "
            "contratual posterior ao valor originalmente empenhado."
        ),
    )

    st.divider()

    st.subheader("Pagamentos vs. Empenhado ao longo do ano")

    # GROUP BY ano+mes (não só mes) — com mais de um ano selecionado na
    # barra lateral, agrupar só por mês somaria "todos os janeiros" de anos
    # diferentes na mesma barra, sem indicar isso no gráfico.
    query_mensal = f"""
        SELECT
            dt.ano,
            dt.mes,
            SUM(fc.valor_pago) AS valor_pago,
            SUM(fc.valor_empenhado) AS valor_empenhado
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_tempo dt ON fc.sk_tempo = dt.sk_tempo
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
        GROUP BY dt.ano, dt.mes
        ORDER BY dt.ano, dt.mes
    """
    df_mensal = run_query(query_mensal)

    if df_mensal.empty:
        st.info("Nenhum dado encontrado para os filtros atuais.")
    else:
        df_mensal["periodo"] = df_mensal["ano"].astype(str) + "-" + df_mensal["mes"].astype(str).str.zfill(2)
        df_mensal_long = df_mensal.melt(
            id_vars="periodo",
            value_vars=["valor_pago", "valor_empenhado"],
            var_name="tipo",
            value_name="valor",
        )
        df_mensal_long["tipo"] = df_mensal_long["tipo"].map({"valor_pago": "Pago", "valor_empenhado": "Empenhado"})

        fig_mensal = px.line(
            df_mensal_long,
            x="periodo",
            y="valor",
            color="tipo",
            markers=True,
            color_discrete_map={"Pago": COR_PAGO, "Empenhado": COR_EMPENHADO},
            title="Pago vs. Empenhado por mês",
            labels={"valor": "Valor (R$)", "periodo": "Mês", "tipo": "Origem"},
        )
        fig_mensal.update_traces(line=dict(width=3), marker=dict(size=8))
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
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
        GROUP BY dorg.nome, dorg.sigla
    """
    df_aproveitamento = run_query(query_aproveitamento)

    if df_aproveitamento.empty:
        st.info("Nenhum órgão encontrado para os filtros atuais.")
        return

    st.subheader("Top 10 órgãos por valor pago")
    top10_valor_pago = df_aproveitamento.nlargest(10, "total_pago")
    fig_top10_valor = px.bar(
        top10_valor_pago,
        x="total_pago",
        y="nome_orgao",
        orientation="h",
        labels={"total_pago": "Valor pago (R$)", "nome_orgao": "Órgão"},
        color_discrete_sequence=[COR_PAGO],
    )
    fig_top10_valor.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_top10_valor, use_container_width=True)

    st.divider()

    # aproveitamento_pct calculado pra TODOS os órgãos (mesmo abaixo do corte
    # de R$100mi) — o corte só define quem entra no "top 5" usado como
    # sugestão padrão do seletor abaixo, não quem pode ser escolhido nele.
    df_aproveitamento["aproveitamento_pct"] = df_aproveitamento.apply(
        lambda r: (r["total_pago"] / r["total_empenhado"] * 100) if r["total_empenhado"] else 0.0, axis=1
    )
    top5_aproveitamento = (
        df_aproveitamento[df_aproveitamento["total_empenhado"] > CORTE_TOP5_APROVEITAMENTO]
        .sort_values("aproveitamento_pct", ascending=False)
        .head(5)
    )

    todos_orgaos = sorted(df_aproveitamento["nome_orgao"].tolist())
    orgao_padrao = top5_aproveitamento.iloc[0]["nome_orgao"] if not top5_aproveitamento.empty else todos_orgaos[0]
    indice_padrao = todos_orgaos.index(orgao_padrao) if orgao_padrao in todos_orgaos else 0

    col_titulo, col_select = st.columns([2, 1])

    with col_select:
        orgao_escolhido = st.selectbox(
            "Órgão",
            options=todos_orgaos,
            index=indice_padrao,
            key="orgao_aproveitamento_selecionado",
            help=(
                "Lista todos os órgãos do período filtrado. Por padrão já vem selecionado o de maior "
                "% de aproveitamento entre os que empenharam mais de R$ 100 milhões — evita que um "
                'órgão pequeno apareça como "o melhor" só por ter um denominador baixo.'
            ),
        )

    with col_titulo:
        st.subheader(orgao_escolhido)

    linha_orgao = df_aproveitamento[df_aproveitamento["nome_orgao"] == orgao_escolhido].iloc[0]

    nome_escapado = orgao_escolhido.replace("'", "''")
    query_evolucao_orgao = f"""
        SELECT
            dt.ano,
            dt.mes,
            SUM(fc.valor_empenhado) AS valor_empenhado,
            SUM(fc.valor_pago) AS valor_pago
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_tempo dt ON fc.sk_tempo = dt.sk_tempo
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE dorg.nome = '{nome_escapado}'
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        GROUP BY dt.ano, dt.mes
        ORDER BY dt.ano, dt.mes
    """
    df_evolucao_orgao = run_query(query_evolucao_orgao)

    col_grafico, col_pct = st.columns([3, 1])

    with col_grafico:
        if df_evolucao_orgao.empty:
            st.info("Nenhum dado mensal encontrado para este órgão.")
        else:
            df_evolucao_orgao["periodo"] = (
                df_evolucao_orgao["ano"].astype(str) + "-" + df_evolucao_orgao["mes"].astype(str).str.zfill(2)
            )
            df_evolucao_long = df_evolucao_orgao.melt(
                id_vars="periodo",
                value_vars=["valor_empenhado", "valor_pago"],
                var_name="tipo",
                value_name="valor",
            )
            df_evolucao_long["tipo"] = df_evolucao_long["tipo"].map(
                {"valor_empenhado": "Empenhado", "valor_pago": "Pago"}
            )

            fig_evolucao = px.line(
                df_evolucao_long,
                x="periodo",
                y="valor",
                color="tipo",
                markers=True,
                color_discrete_map={"Empenhado": COR_EMPENHADO, "Pago": COR_PAGO},
                title=f"Empenhado vs. Pago por mês — {orgao_escolhido}",
                labels={"valor": "Valor (R$)", "periodo": "Mês", "tipo": "Origem"},
            )
            fig_evolucao.update_traces(line=dict(width=3), marker=dict(size=8))
            st.plotly_chart(fig_evolucao, use_container_width=True)

    with col_pct:
        st.metric(
            "Aproveitamento do empenho",
            f"{linha_orgao['aproveitamento_pct']:.1f}%",
            help="% do valor empenhado por este órgão que já foi efetivamente pago.",
        )
        st.metric("Total empenhado", formatar_moeda_adaptativo(linha_orgao["total_empenhado"]))
        st.metric("Total pago", formatar_moeda_adaptativo(linha_orgao["total_pago"]))
