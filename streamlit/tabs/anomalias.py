"""Aba "Anomalias em Contratos" — score do Modelo 1, faixas de risco, distribuição."""

import numpy as np
import pandas as pd
import plotly.express as px
from config import COR_PAGO, CORES_FAIXA_RISCO, FAIXA_ALTO, FAIXA_BAIXO, FAIXA_MEDIO
from db import run_query
from formatting import classificar_risco, formatar_bilhoes
from sql_filters import anos_filter_sql, orgaos_filter_sql

import streamlit as st


def _faixa_alcancavel(faixa: str, score_threshold: float) -> bool:
    """Uma faixa de risco só é alcançável no histograma se tiver overlap com
    o intervalo [score_threshold, 1.0] — que é o que a query já filtra
    (WHERE fc.score_anomalia >= score_threshold). Ex: com o padrão 0,70,
    "Baixo risco" nunca aparece, porque a query nem traz esses contratos."""
    if faixa == FAIXA_BAIXO:
        return score_threshold < 0.70
    if faixa == FAIXA_MEDIO:
        return score_threshold <= 0.85
    return True  # FAIXA_ALTO — sempre alcançável dentro de [threshold, 1.0]


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

    query_valor_total_contratos = f"""
        SELECT SUM(fc.valor_contrato) AS valor_total
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
        WHERE 1=1
        {anos_filter_sql(anos_selecionados, "fc.ano")}
        {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
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
        return

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
        # A query já filtra score_anomalia >= score_threshold — faixas abaixo
        # do limiar do slider nunca aparecem aqui (ex: com o padrão de 0,70,
        # "Baixo risco" nunca é alcançável). Restringe a legenda/cores só às
        # faixas que de fato podem aparecer, em vez de mostrar uma faixa que
        # estruturalmente nunca tem barra — evita a leitura errada de "não
        # há contrato de baixo risco" quando na verdade ele só não foi nem
        # consultado.
        faixas_alcancaveis = [
            f for f in (FAIXA_BAIXO, FAIXA_MEDIO, FAIXA_ALTO) if _faixa_alcancavel(f, score_threshold)
        ]

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
        # Remove os bins abaixo do próprio limiar do slider — o histograma,
        # do jeito que a query está montada, só teria barra zerada ali mesmo.
        df_hist = df_hist[df_hist["centro"] >= score_threshold]

        st.caption(
            f"Distribuição já restrita a score ≥ {score_threshold:.2f} (filtro da barra lateral). "
            "Para ver a distribuição completa, incluindo risco baixo, reduza o filtro para 0."
        )

        fig_hist = px.bar(
            df_hist,
            x="centro",
            y="contratos",
            color="faixa",
            color_discrete_map=CORES_FAIXA_RISCO,
            category_orders={"faixa": faixas_alcancaveis},
            title="Distribuição do score de anomalia (dentro do filtro atual)",
            labels={"centro": "Score de anomalia", "contratos": "Nº de contratos", "faixa": "Risco"},
        )
        fig_hist.update_traces(marker_line_width=0)
        fig_hist.update_layout(bargap=0.02)
        st.plotly_chart(fig_hist, use_container_width=True)

    st.subheader("Contratos com maior score de anomalia")
    LIMITE_TABELA = 500
    if len(df_anom) > LIMITE_TABELA:
        total_fmt = f"{len(df_anom):,}".replace(",", ".")
        st.caption(
            f"Mostrando os {LIMITE_TABELA} contratos de maior score, de {total_fmt} encontrados "
            "no filtro atual. Aumente o score mínimo na barra lateral para reduzir a lista."
        )
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
        ].head(LIMITE_TABELA),
        use_container_width=True,
        hide_index=True,
    )
