"""Aba "Previsão de Pagamentos" — Modelo 2 (XGBoost quantílico).

`render()` retorna (ano_ref, trimestre_previsto_sel) — a aba "Resumo (IA)"
reaproveita essa mesma referência de ano/trimestre escolhida aqui.
"""

import pandas as pd
import plotly.express as px
from config import COR_PAGO, COR_PREVISTO
from db import run_query
from formatting import formatar_bilhoes

import streamlit as st


def _primeiro_trimestre_sem_dado_real(real_map: dict) -> int:
    """Primeiro trimestre (1, 2 ou 3) sem pagamento real registrado — usado
    como padrão do seletor, pra não sugerir substituir um trimestre que já
    tem dado real conhecido pela previsão do modelo."""
    for t in (1, 2, 3):
        if not real_map.get(t):
            return t
    return 3  # os 3 já têm dado real — não há trimestre "óbvio" a prever


def render(anos_disponiveis: list) -> tuple[int, int]:
    st.subheader("Valor pago por trimestre")

    query_anos_previstos = "SELECT DISTINCT ano_previsto FROM iceberg.ml.previsao_pagamento_orgao"
    df_anos_previstos = run_query(query_anos_previstos)
    anos_previstos = set(df_anos_previstos["ano_previsto"].tolist()) if not df_anos_previstos.empty else set()

    ano_ref = st.selectbox(
        "Ano de referência",
        options=sorted(set(anos_disponiveis) | anos_previstos, reverse=True),
        index=0,
    )

    # Consulta o dado real ANTES do seletor de trimestre, pra sugerir como
    # padrão o primeiro trimestre que ainda não tem pagamento real — evita
    # que o padrão substitua, pela previsão, um trimestre cujo valor real
    # já é conhecido (achado real: 2026-T1/T2/T3 já tinham dado real E
    # previsão ao mesmo tempo, e o padrão antigo — sempre T3 — mostrava a
    # previsão no lugar do valor real sem nenhum aviso).
    query_empenho_trimestre = f"""
        SELECT dt.trimestre, SUM(fc.valor_pago) AS valor_pago_trimestre
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_tempo dt ON fc.sk_tempo = dt.sk_tempo
        WHERE fc.ano = {ano_ref}
        GROUP BY dt.trimestre
        ORDER BY dt.trimestre
    """
    df_emp_tri = run_query(query_empenho_trimestre)
    real_map = (
        dict(zip(df_emp_tri["trimestre"], df_emp_tri["valor_pago_trimestre"], strict=False))
        if not df_emp_tri.empty
        else {}
    )

    trimestre_previsto_sel = st.selectbox(
        "Trimestre a substituir pela previsão",
        options=[1, 2, 3],
        index=_primeiro_trimestre_sem_dado_real(real_map) - 1,
        help="Por padrão, sugere o primeiro trimestre do ano que ainda não tem pagamento real registrado.",
    )

    st.caption(
        "Soma total de todos os órgãos por trimestre. Os trimestres com dado real usam "
        "o valor efetivamente pago; o trimestre selecionado acima usa a previsão do "
        "modelo (mediana, com intervalo P10-P90)."
    )

    if real_map.get(trimestre_previsto_sel):
        st.warning(
            f"⚠️ Já existe pagamento real registrado para {ano_ref}-T{trimestre_previsto_sel} "
            f"({formatar_bilhoes(real_map[trimestre_previsto_sel])}). O gráfico abaixo está "
            "mostrando a **previsão** no lugar do valor real só para fins de comparação — "
            "não é o valor efetivamente pago.",
            icon="⚠️",
        )

    query_previsto_trimestre = f"""
        SELECT SUM(valor_previsto_p50) AS p50
        FROM iceberg.ml.previsao_pagamento_orgao
        WHERE ano_previsto = {ano_ref}
          AND trimestre_previsto = {trimestre_previsto_sel}
    """
    df_prev_tri = run_query(query_previsto_trimestre)

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
        "aqui os dois valores aparecem lado a lado para cada trimestre disponível na tabela de previsão — "
        "inclusive nos trimestres onde os dois já existem, útil pra comparar o quão perto a previsão chegou do real."
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
        return ano_ref, trimestre_previsto_sel

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

    return ano_ref, trimestre_previsto_sel
