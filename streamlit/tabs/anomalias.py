"""Aba "Anomalias em Contratos" — score do Modelo 1, faixas de risco, distribuição."""

import numpy as np
import pandas as pd
import plotly.express as px
from config import COR_PAGO, CORES_FAIXA_RISCO, FAIXA_ALTO, FAIXA_BAIXO, FAIXA_MEDIO
from db import run_query
from formatting import classificar_risco, formatar_bilhoes
from sql_filters import anos_filter_sql, orgaos_filter_sql

import streamlit as st

LIMITE_TABELA = 500

# Score mínimo pra ENTRAR no histograma "Distribuição do score de anomalia"
# — a maioria dos ~216 mil contratos tem score bem baixo (perto de 0), o que
# achata visualmente a parte que interessa (perto de 0,70/0,85). Corte só
# visual: não mexe no score_threshold da sidebar (que ainda filtra os KPIs
# e a tabela abaixo).
SCORE_MINIMO_HISTOGRAMA = 0.49


def render(anos_selecionados: list, orgaos_selecionados: list, score_threshold: float) -> None:
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
        LEFT JOIN iceberg.gold.dim_credor dcred ON fc.sk_credor = dcred.sk_credor
        JOIN iceberg.gold.dim_modalidade dmod ON fc.sk_modalidade = dmod.sk_modalidade
        LEFT JOIN iceberg.ml.score_anomalia_contrato sac
            ON fc.id_contrato_origem = sac.id_contrato_origem AND fc.ano = sac.ano
        WHERE fc.score_anomalia >= {score_threshold}
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
        ORDER BY fc.score_anomalia DESC
    """

    # Distribuição (score >= SCORE_MINIMO_HISTOGRAMA, ver constante) — colunas
    # completas de contrato, não só o score: reaproveitada pela tabela que
    # filtra ao clicar numa coluna do histograma logo abaixo. Independente do
    # score mínimo escolhido na barra lateral — o slider filtra a tabela de
    # KPIs mais abaixo, não o quadro geral de como os scores altos se distribuem.
    query_distribuicao_completa = f"""
        SELECT
            fc.id_contrato_origem,
            fc.ano,
            fc.valor_contrato,
            fc.valor_pago,
            fc.status,
            fc.flag_emergency,
            fc.score_anomalia,
            dorg.nome AS nome_orgao,
            dcred.nome AS nome_credor,
            dcred.tipo AS tipo_credor,
            dmod.descricao_modalidade
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        LEFT JOIN iceberg.gold.dim_credor dcred ON fc.sk_credor = dcred.sk_credor
        JOIN iceberg.gold.dim_modalidade dmod ON fc.sk_modalidade = dmod.sk_modalidade
        WHERE fc.score_anomalia >= {SCORE_MINIMO_HISTOGRAMA}
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
        ORDER BY fc.score_anomalia DESC
    """

    query_valor_total_contratos = f"""
        SELECT SUM(fc.valor_contrato) AS valor_total, COUNT(DISTINCT dorg.nome) AS total_orgaos
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
    """

    # Evolução por ano — de propósito SEM o filtro de ano da barra lateral
    # (é o contexto histórico que a visão de um único ano não mostra); com o
    # filtro de órgão aplicado, pra respeitar o recorte que o usuário já fez.
    query_evolucao_anual = f"""
        SELECT
            fc.ano,
            COUNT(*) AS total_contratos,
            SUM(CASE WHEN fc.score_anomalia >= 0.70 THEN 1 ELSE 0 END) AS n_anomalos
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE fc.score_anomalia IS NOT NULL
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
        GROUP BY fc.ano
        ORDER BY fc.ano
    """

    df_anom = run_query(query_anomalias)
    df_dist_completa = run_query(query_distribuicao_completa)
    df_valor_total = run_query(query_valor_total_contratos)
    df_evolucao_anual = run_query(query_evolucao_anual)
    valor_total_contratos = (
        df_valor_total.iloc[0]["valor_total"]
        if not df_valor_total.empty and pd.notna(df_valor_total.iloc[0]["valor_total"])
        else 0
    )
    total_orgaos_periodo = (
        int(df_valor_total.iloc[0]["total_orgaos"])
        if not df_valor_total.empty and pd.notna(df_valor_total.iloc[0]["total_orgaos"])
        else 0
    )

    st.subheader("Distribuição do score de anomalia")
    st.caption(
        "Todos os contratos escorados no período filtrado, independente do score mínimo abaixo. "
        "Modelo: Isolation Forest."
    )
    if df_dist_completa.empty:
        st.info(f"Nenhum contrato com score ≥ {SCORE_MINIMO_HISTOGRAMA:.2f} para os filtros atuais.")
    else:
        bin_edges = np.arange(0, 1.05, 0.05)
        contagens, edges = np.histogram(df_dist_completa["score_anomalia"].astype(float), bins=bin_edges)
        centros = (edges[:-1] + edges[1:]) / 2
        df_hist = pd.DataFrame(
            {
                "centro": centros,
                "contratos": contagens,
                "faixa": [classificar_risco(c) for c in centros],
            }
        )
        # Bins alinhados ao grid de 0,05 desde 0 (mesmo dos limiares 0,70/0,85),
        # mas só exibidos a partir de SCORE_MINIMO_HISTOGRAMA — o resto fica
        # vazio (já filtrado na query) e só ocuparia espaço sem informação.
        df_hist = df_hist[df_hist["centro"] >= SCORE_MINIMO_HISTOGRAMA]
        fig_hist = px.bar(
            df_hist,
            x="centro",
            y="contratos",
            color="faixa",
            color_discrete_map=CORES_FAIXA_RISCO,
            category_orders={"faixa": [FAIXA_BAIXO, FAIXA_MEDIO, FAIXA_ALTO]},
            labels={"centro": "Score de anomalia", "contratos": "Nº de contratos", "faixa": "Risco"},
        )
        fig_hist.add_vline(
            x=score_threshold,
            line_dash="dash",
            line_color="gray",
            annotation_text=f"score mínimo atual: {score_threshold:.2f}",
            annotation_position="top",
        )
        fig_hist.update_traces(marker_line_width=0)
        fig_hist.update_layout(bargap=0.02)
        evento_hist = st.plotly_chart(
            fig_hist,
            use_container_width=True,
            on_select="rerun",
            selection_mode=["points"],
            key="hist_score_anomalia",
        )

        largura_bin = float(bin_edges[1] - bin_edges[0])
        pontos_clicados = evento_hist["selection"]["points"] if evento_hist else []
        if pontos_clicados:
            centro_clicado = pontos_clicados[0]["x"]
            low, high = centro_clicado - largura_bin / 2, centro_clicado + largura_bin / 2
            st.subheader(f"Contratos com score entre {low:.2f} e {high:.2f}")
            df_bin = df_dist_completa[
                (df_dist_completa["score_anomalia"] >= low) & (df_dist_completa["score_anomalia"] < high)
            ]
            if df_bin.empty:
                st.info("Nenhum contrato nessa faixa de score.")
            else:
                st.dataframe(
                    df_bin[
                        [
                            "id_contrato_origem",
                            "ano",
                            "nome_orgao",
                            "nome_credor",
                            "tipo_credor",
                            "descricao_modalidade",
                            "valor_contrato",
                            "valor_pago",
                            "score_anomalia",
                            "status",
                            "flag_emergency",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.caption("Clique em uma coluna do gráfico acima para ver os contratos daquela faixa de score.")

    st.divider()

    if df_anom.empty:
        st.info("Nenhum contrato acima do score mínimo com os filtros atuais. Tente reduzir o score mínimo.")
        return

    df_anom["faixa_risco"] = df_anom["score_anomalia"].astype(float).apply(classificar_risco)
    df_medio_alto = df_anom[df_anom["faixa_risco"].isin([FAIXA_MEDIO, FAIXA_ALTO])]

    valor_anomalos = df_medio_alto["valor_contrato"].sum()
    pct_valor_anomalo = (valor_anomalos / valor_total_contratos * 100) if valor_total_contratos else 0

    st.subheader(f"Contratos com score ≥ {score_threshold:.2f}")
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

    col5, col6, col7 = st.columns(3)
    col5.metric("Órgãos afetados (médio + alto risco)", df_medio_alto["nome_orgao"].nunique())
    col6.metric(
        "Total de órgãos",
        total_orgaos_periodo,
        help="Total de órgãos com contrato no período filtrado (ano/órgão), independente do score.",
    )
    col7.metric(
        "Score médio (médio + alto risco)",
        f"{df_medio_alto['score_anomalia'].mean():.3f}" if not df_medio_alto.empty else "N/D",
    )

    st.divider()

    colA, colB = st.columns(2)
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
        if df_evolucao_anual.empty:
            st.info("Sem histórico suficiente para a evolução anual.")
        else:
            df_evolucao_anual["pct_anomalos"] = (
                df_evolucao_anual["n_anomalos"] / df_evolucao_anual["total_contratos"] * 100
            )
            fig_evolucao = px.bar(
                df_evolucao_anual,
                x="ano",
                y="pct_anomalos",
                title="Evolução anual do % de contratos anômalos",
                labels={"ano": "Ano", "pct_anomalos": "% de contratos (score ≥ 0,70)"},
                color_discrete_sequence=["#e74c3c"],
                text_auto=".1f",
            )
            fig_evolucao.update_xaxes(type="category")
            fig_evolucao.update_traces(texttemplate="%{y:.1f}%", textposition="outside")
            st.plotly_chart(fig_evolucao, use_container_width=True)

    st.subheader("Contratos com maior score de anomalia")
    if len(df_anom) > LIMITE_TABELA:
        total_fmt = f"{len(df_anom):,}".replace(",", ".")
        st.caption(f"Top {LIMITE_TABELA} de {total_fmt} contratos — aumente o score mínimo para uma lista mais curta.")
    st.dataframe(
        df_anom[
            [
                "id_contrato_origem",
                "ano",
                "nome_orgao",
                "nome_credor",
                "tipo_credor",
                "descricao_modalidade",
                "valor_contrato",
                "valor_pago",
                "score_anomalia",
                "status",
                "flag_emergency",
            ]
        ].head(LIMITE_TABELA),
        use_container_width=True,
        hide_index=True,
    )
