"""Aba "Previsão de Pagamentos" — Modelo 2 (XGBoost quantílico).

Mostra sempre o dado real onde ele existe e a previsão onde não existe —
sem "substituir" um pelo outro. Nos trimestres em que os dois já existem
(ex: modelo rodou antes do trimestre fechar), mostra os dois lado a lado e
calcula o erro do modelo — uma checagem honesta de acurácia, não um
problema a esconder.

`render()` retorna (ano_ref, trimestre_detalhe) — a aba "Resumo (IA)"
reaproveita essa mesma referência.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import COR_PAGO, COR_PREVISTO
from db import run_query
from formatting import formatar_bilhoes

import streamlit as st


def render(anos_disponiveis: list) -> tuple[int, int]:
    st.subheader("Valor pago por trimestre — real e previsto")

    df_anos_previstos = run_query("SELECT DISTINCT ano_previsto FROM iceberg.ml.previsao_pagamento_orgao")
    anos_previstos = set(df_anos_previstos["ano_previsto"].tolist()) if not df_anos_previstos.empty else set()

    ano_ref = st.selectbox(
        "Ano de referência",
        options=sorted(set(anos_disponiveis) | anos_previstos, reverse=True),
        index=0,
    )

    df_real = run_query(
        f"""
        SELECT dt.trimestre, SUM(fc.valor_pago) AS valor
        FROM iceberg.gold.fato_contrato fc
        JOIN iceberg.gold.dim_tempo dt ON fc.sk_tempo = dt.sk_tempo
        WHERE fc.ano = {ano_ref}
        GROUP BY dt.trimestre
        """
    )
    real_map = dict(zip(df_real["trimestre"], df_real["valor"], strict=False)) if not df_real.empty else {}

    df_prev = run_query(
        f"""
        SELECT trimestre_previsto,
               SUM(valor_previsto_p10) AS p10,
               SUM(valor_previsto_p50) AS p50,
               SUM(valor_previsto_p90) AS p90
        FROM iceberg.ml.previsao_pagamento_orgao
        WHERE ano_previsto = {ano_ref}
        GROUP BY trimestre_previsto
        """
    )
    prev_map = (
        {int(row.trimestre_previsto): (row.p10, row.p50, row.p90) for row in df_prev.itertuples()}
        if not df_prev.empty
        else {}
    )

    trimestres = sorted(set(real_map) | set(prev_map))
    if not trimestres:
        st.info(f"Nenhum dado de pagamento ou previsão encontrado para {ano_ref}.")
        return ano_ref, 1

    # --- Gráfico único: barra "Pago (real)" onde existe, "Previsto" (com
    # intervalo P10-P90) onde existe — os dois juntos no mesmo trimestre
    # quando os dois existem, nunca um escondendo o outro.
    fig = go.Figure()
    labels = [f"T{t}" for t in trimestres]

    reais = [real_map.get(t) for t in trimestres]
    fig.add_bar(name="Pago (real)", x=labels, y=reais, marker_color=COR_PAGO)

    previstos_p50 = [prev_map[t][1] if t in prev_map else None for t in trimestres]
    erro_mais = [prev_map[t][2] - prev_map[t][1] if t in prev_map else None for t in trimestres]
    erro_menos = [prev_map[t][1] - prev_map[t][0] if t in prev_map else None for t in trimestres]
    fig.add_bar(
        name="Previsto (mediana, P10-P90)",
        x=labels,
        y=previstos_p50,
        marker_color=COR_PREVISTO,
        error_y=dict(type="data", symmetric=False, array=erro_mais, arrayminus=erro_menos),
    )
    fig.update_layout(
        barmode="group",
        title=f"Pago vs. Previsto por trimestre — {ano_ref}",
        xaxis_title="Trimestre",
        yaxis_title="Valor (R$)",
        legend_title="Origem",
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Ver números exatos por trimestre"):
        st.dataframe(
            pd.DataFrame(
                {
                    "trimestre": labels,
                    "pago (real)": reais,
                    "previsto (p10)": [prev_map[t][0] if t in prev_map else None for t in trimestres],
                    "previsto (mediana)": previstos_p50,
                    "previsto (p90)": [prev_map[t][2] if t in prev_map else None for t in trimestres],
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    # --- KPIs: total real do ano sempre à parte da previsão — nunca somados
    # como se fossem a mesma coisa.
    total_real_ano = sum(v for v in real_map.values() if v)
    trimestres_so_previstos = [t for t in trimestres if t in prev_map and not real_map.get(t)]

    col1, col2, col3 = st.columns(3)
    col1.metric(f"Total pago em {ano_ref} (real)", formatar_bilhoes(total_real_ano))
    if trimestres_so_previstos:
        total_previsto_futuro = sum(prev_map[t][1] for t in trimestres_so_previstos)
        col2.metric(
            f"Previsão p/ trimestre(s) sem dado real ({', '.join('T' + str(t) for t in trimestres_so_previstos)})",
            formatar_bilhoes(total_previsto_futuro),
        )
    else:
        col2.metric("Previsão p/ trimestre(s) sem dado real", "—", help="Todos os trimestres já têm dado real.")

    trimestres_com_ambos = [t for t in trimestres if t in prev_map and real_map.get(t)]
    if trimestres_com_ambos:
        erros_pct = [abs(real_map[t] - prev_map[t][1]) / real_map[t] * 100 for t in trimestres_com_ambos]
        col3.metric(
            "Erro médio do modelo (trimestres já fechados)",
            f"{sum(erros_pct) / len(erros_pct):.1f}%",
            help=(
                "Diferença percentual entre a previsão (mediana) e o valor real, calculada só "
                f"nos trimestres onde os dois já existem: {', '.join('T' + str(t) for t in trimestres_com_ambos)}."
            ),
        )
    else:
        col3.metric("Erro médio do modelo", "N/D", help="Nenhum trimestre com real e previsão ao mesmo tempo ainda.")

    st.divider()

    # --- Detalhe por órgão: só entre os trimestres que de fato têm previsão.
    trimestres_com_previsao = sorted(prev_map.keys())
    if not trimestres_com_previsao:
        return ano_ref, trimestres[-1]

    trimestre_detalhe = st.selectbox(
        "Ver detalhe da previsão por órgão para o trimestre:",
        options=trimestres_com_previsao,
        index=len(trimestres_com_previsao) - 1,
        format_func=lambda t: f"T{t}" + (" (já tem dado real também)" if real_map.get(t) else ""),
    )

    query_top5 = f"""
        SELECT nome_orgao, valor_previsto_p10, valor_previsto_p50, valor_previsto_p90
        FROM iceberg.ml.previsao_pagamento_orgao
        WHERE ano_previsto = {ano_ref} AND trimestre_previsto = {trimestre_detalhe}
        ORDER BY valor_previsto_p50 DESC
        LIMIT 5
    """
    df_top5 = run_query(query_top5)

    if df_top5.empty:
        st.info("Nenhuma previsão encontrada para o trimestre selecionado.")
    else:
        df_top5["erro_mais"] = df_top5["valor_previsto_p90"] - df_top5["valor_previsto_p50"]
        df_top5["erro_menos"] = df_top5["valor_previsto_p50"] - df_top5["valor_previsto_p10"]

        col_esq, col_dir = st.columns(2)
        with col_esq:
            fig_top5 = px.bar(
                df_top5,
                x="nome_orgao",
                y="valor_previsto_p50",
                error_y="erro_mais",
                error_y_minus="erro_menos",
                title=f"Top 5 pagamentos previstos — {ano_ref}-T{trimestre_detalhe} (erro = P10-P90)",
                labels={"valor_previsto_p50": "Valor previsto (mediana, R$)", "nome_orgao": "Órgão"},
                color_discrete_sequence=[COR_PREVISTO],
            )
            fig_top5.update_layout(xaxis_tickangle=-30)
            st.plotly_chart(fig_top5, use_container_width=True)

        with col_dir:
            st.dataframe(
                df_top5[["nome_orgao", "valor_previsto_p10", "valor_previsto_p50", "valor_previsto_p90"]],
                use_container_width=True,
                hide_index=True,
            )

    return ano_ref, trimestre_detalhe
