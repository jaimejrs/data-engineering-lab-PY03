"""Aba "Previsão de Pagamentos" — Modelo 2 (XGBoost quantílico).

O modelo (`models/payment_forecast.py`) treina sobre `fato_ordem_bancaria`
(pagamento efetivo ao credor, 3º estágio da despesa) — por isso o "real" desta
aba compara com a MESMA fonte, não com `fato_contrato.valor_pago` (atributo do
próprio contrato, que a nota "Sobre os dados" no topo do painel já avisa que
só reconcilia com ordem bancária em ~7-8% dos casos). Comparar com a fonte
errada foi o que produzia erro de modelo na casa de 100.000%+ numa versão
anterior desta aba — não era o modelo que estava ruim, era a comparação.

Segunda particularidade do Modelo 2: a previsão é por ÓRGÃO, e "próximo
trimestre" é relativo ao último trimestre em que CADA órgão tem dado —
não um corte único para o governo inteiro. Órgãos com atividade contínua
até o trimestre mais recente caem todos no mesmo "próximo trimestre"
(cobertura alta); órgãos com histórico mais antigo ou intermitente geram
previsões para trimestres passados, cobrindo só a si mesmos (cobertura
baixa). Por isso todo trimestre com previsão mostra sua cobertura (nº de
órgãos previstos / universo de órgãos com ordem bancária) — sem isso, somar
"previsto" de um trimestre de baixa cobertura contra o "real" do governo
inteiro sub-representa o modelo por um fator de dezenas a centenas de vezes.

Previsão retroativa (30/07/2026, pedido explícito): além da previsão real
(pro próximo trimestre de cada órgão, alvo desconhecido), `models/payment_
forecast.py` também grava previsão retroativa pros últimos trimestres que já
fecharam (treinada só com dado anterior a cada um, sem vazamento — ver
`forecast_quarters_backtest`). É isso que permite comparar "previsto vs.
realizado" nesta aba mesmo pra trimestres que já têm dado real, sem esperar
um novo fechar.

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

# Cobertura mínima (previsto / universo de órgãos) para um trimestre entrar no
# cálculo de "erro médio do modelo" — trimestres abaixo disso comparariam a
# previsão de um punhado de órgãos contra o total do governo, o que não mede
# acurácia do modelo, só a diferença de escopo.
COBERTURA_MINIMA_BACKTEST = 0.5


def render(anos_disponiveis: list) -> tuple[int, int]:
    st.subheader("Valor pago por trimestre — real (ordem bancária) e previsto")

    df_modelo_info = run_query("SELECT MAX(scored_at) AS ultimo FROM iceberg.ml.previsao_pagamento_orgao")
    if not df_modelo_info.empty and pd.notna(df_modelo_info.iloc[0]["ultimo"]):
        ultimo_treino = df_modelo_info.iloc[0]["ultimo"]
        st.caption(f"Modelo (XGBoost quantílico) atualizado em {ultimo_treino:%d/%m/%Y %H:%M}.")

    df_universo = run_query("""
        SELECT COUNT(DISTINCT o.codigo) AS total_orgaos
        FROM iceberg.gold.fato_ordem_bancaria fob
        JOIN iceberg.gold.dim_orgao o ON fob.sk_orgao = o.sk_orgao
        WHERE NOT fob.flag_cancelada
    """)
    total_orgaos_universo = int(df_universo.iloc[0]["total_orgaos"]) if not df_universo.empty else 0

    df_anos_previstos = run_query("SELECT DISTINCT ano_previsto FROM iceberg.ml.previsao_pagamento_orgao")
    anos_previstos = set(df_anos_previstos["ano_previsto"].tolist()) if not df_anos_previstos.empty else set()

    ano_ref = st.selectbox(
        "Ano de referência",
        options=sorted(set(anos_disponiveis) | anos_previstos, reverse=True),
        index=0,
    )

    df_real = run_query(
        f"""
        SELECT dt.trimestre, SUM(fob.valor) AS valor
        FROM iceberg.gold.fato_ordem_bancaria fob
        JOIN iceberg.gold.dim_tempo dt ON fob.sk_tempo = dt.sk_tempo
        WHERE dt.ano = {ano_ref} AND NOT fob.flag_cancelada
        GROUP BY dt.trimestre
        """
    )
    real_map = dict(zip(df_real["trimestre"], df_real["valor"], strict=False)) if not df_real.empty else {}

    df_prev = run_query(
        f"""
        SELECT trimestre_previsto,
               SUM(valor_previsto_p10) AS p10,
               SUM(valor_previsto_p50) AS p50,
               SUM(valor_previsto_p90) AS p90,
               COUNT(DISTINCT codigo_orgao) AS n_orgaos
        FROM iceberg.ml.previsao_pagamento_orgao
        WHERE ano_previsto = {ano_ref}
        GROUP BY trimestre_previsto
        """
    )
    prev_map = (
        {int(row.trimestre_previsto): (row.p10, row.p50, row.p90, int(row.n_orgaos)) for row in df_prev.itertuples()}
        if not df_prev.empty
        else {}
    )

    trimestres = sorted(set(real_map) | set(prev_map))
    if not trimestres:
        st.info(f"Nenhum dado de pagamento ou previsão encontrado para {ano_ref}.")
        return ano_ref, 1

    def cobertura(t: int) -> float:
        if t not in prev_map or not total_orgaos_universo:
            return 0.0
        return prev_map[t][3] / total_orgaos_universo

    # --- Gráfico único: barra "Pago (real)" e "Previsto" SOBREPOSTAS no mesmo
    # trimestre (barmode="overlay" + opacidade), não lado a lado — dá pra ver
    # as duas mesmo quando uma é maior que a outra, sem a barra menor sumir
    # atrás da maior. Trimestres com previsão de baixa cobertura (poucos
    # órgãos) ficam com um hachurado + aviso no hover, em vez de aparecer
    # como se fosse a previsão do governo inteiro.
    fig = go.Figure()
    labels = [f"T{t}" for t in trimestres]

    reais = [real_map.get(t) for t in trimestres]
    fig.add_bar(
        name="Pago (real, ordem bancária)",
        x=labels,
        y=reais,
        marker_color=COR_PAGO,
        opacity=0.75,
        hovertemplate="%{x}<br>Pago (real): R$ %{y:,.0f}<extra></extra>",
    )

    previstos_p50 = [prev_map[t][1] if t in prev_map else None for t in trimestres]
    erro_mais = [prev_map[t][2] - prev_map[t][1] if t in prev_map else None for t in trimestres]
    erro_menos = [prev_map[t][1] - prev_map[t][0] if t in prev_map else None for t in trimestres]
    n_orgaos_txt = [f"{prev_map[t][3]}/{total_orgaos_universo} órgãos" if t in prev_map else "" for t in trimestres]
    pattern_shape = ["/" if t in prev_map and cobertura(t) < COBERTURA_MINIMA_BACKTEST else "" for t in trimestres]
    fig.add_bar(
        name="Previsto (mediana, P10-P90)",
        x=labels,
        y=previstos_p50,
        marker_color=COR_PREVISTO,
        opacity=0.75,
        marker_pattern_shape=pattern_shape,
        error_y=dict(type="data", symmetric=False, array=erro_mais, arrayminus=erro_menos),
        customdata=n_orgaos_txt,
        hovertemplate="%{x}<br>Previsto (mediana): R$ %{y:,.0f}<br>Cobertura: %{customdata}<extra></extra>",
    )
    fig.update_layout(
        barmode="overlay",
        bargap=0.35,
        title=f"Pago vs. Previsto por trimestre — {ano_ref}",
        xaxis_title="Trimestre",
        yaxis_title="Valor (R$)",
        legend_title="Origem",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Barras sobrepostas (semitransparentes) para comparar diretamente real e previsto no mesmo "
        "trimestre. Hachura = previsão de baixa cobertura (poucos órgãos com histórico recente o "
        "suficiente para gerar previsão nesse trimestre) — passe o mouse para ver a cobertura exata."
    )

    with st.expander("Ver números exatos por trimestre"):
        st.dataframe(
            pd.DataFrame(
                {
                    "trimestre": labels,
                    "pago (real, ordem bancária)": reais,
                    "previsto (p10)": [prev_map[t][0] if t in prev_map else None for t in trimestres],
                    "previsto (mediana)": previstos_p50,
                    "previsto (p90)": [prev_map[t][2] if t in prev_map else None for t in trimestres],
                    "órgãos previstos": [prev_map[t][3] if t in prev_map else None for t in trimestres],
                    "cobertura": [f"{cobertura(t):.0%}" if t in prev_map else None for t in trimestres],
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    # --- KPIs
    total_real_ano = sum(v for v in real_map.values() if v)
    trimestres_so_previstos = [t for t in trimestres if t in prev_map and not real_map.get(t)]

    col1, col2, col3 = st.columns(3)
    col1.metric(
        f"Total pago em {ano_ref} (real)",
        formatar_bilhoes(total_real_ano),
        help="Soma de fato_ordem_bancaria (pagamento efetivo ao credor), excluindo ordens canceladas.",
    )
    if trimestres_so_previstos:
        # Reporta o trimestre de maior cobertura entre os "sem dado real" —
        # é o que representa de fato "a previsão do próximo trimestre do
        # governo", não um órgão isolado com histórico defasado.
        t_principal = max(trimestres_so_previstos, key=cobertura)
        total_previsto_futuro = prev_map[t_principal][1]
        col2.metric(
            f"Previsão para T{t_principal} (próximo trimestre sem dado real)",
            formatar_bilhoes(total_previsto_futuro),
            help=f"Cobertura: {prev_map[t_principal][3]} de {total_orgaos_universo} órgãos ({cobertura(t_principal):.0%}).",
        )
    else:
        col2.metric("Previsão p/ trimestre(s) sem dado real", "—", help="Todos os trimestres já têm dado real.")

    trimestres_backtest = [
        t for t in trimestres if t in prev_map and real_map.get(t) and cobertura(t) >= COBERTURA_MINIMA_BACKTEST
    ]
    if trimestres_backtest:
        erros_pct = [abs(real_map[t] - prev_map[t][1]) / real_map[t] * 100 for t in trimestres_backtest]
        col3.metric(
            "Erro médio do modelo (trimestres fechados, cobertura ≥ 50%)",
            f"{sum(erros_pct) / len(erros_pct):.1f}%",
            help=(
                "Diferença percentual entre a previsão (mediana) e o valor real, calculada só nos "
                f"trimestres onde os dois existem E a previsão cobre ao menos {COBERTURA_MINIMA_BACKTEST:.0%} "
                f"dos órgãos: {', '.join('T' + str(t) for t in trimestres_backtest)}."
            ),
        )
    else:
        col3.metric(
            "Erro médio do modelo",
            "N/D",
            help=(
                "Nenhum trimestre fechado ainda tem previsão com cobertura suficiente "
                f"(≥ {COBERTURA_MINIMA_BACKTEST:.0%} dos órgãos) para validar a acurácia."
            ),
        )

    st.divider()

    # --- Histórico plurianual: contexto que a visão de um único ano (acima)
    # não dá — de propósito não filtrado por `ano_ref`, mostra todos os anos
    # com ordem bancária disponível.
    df_historico = run_query("""
        SELECT dt.ano, dt.trimestre, SUM(fob.valor) AS valor
        FROM iceberg.gold.fato_ordem_bancaria fob
        JOIN iceberg.gold.dim_tempo dt ON fob.sk_tempo = dt.sk_tempo
        WHERE NOT fob.flag_cancelada
        GROUP BY dt.ano, dt.trimestre
        ORDER BY dt.ano, dt.trimestre
    """)
    if not df_historico.empty:
        st.subheader("Histórico: total pago por trimestre (todos os anos)")
        df_historico["periodo"] = df_historico["ano"].astype(str) + "-T" + df_historico["trimestre"].astype(str)
        fig_historico = px.line(
            df_historico,
            x="periodo",
            y="valor",
            markers=True,
            labels={"valor": "Valor pago (R$)", "periodo": "Trimestre"},
            color_discrete_sequence=[COR_PAGO],
        )
        fig_historico.update_traces(line=dict(width=3), marker=dict(size=7))
        st.plotly_chart(fig_historico, use_container_width=True)
        st.caption("Todos os trimestres com ordem bancária disponível — contexto de tendência, não filtrado por ano.")

        st.divider()

    # --- Detalhe por órgão: só entre os trimestres que de fato têm previsão.
    trimestres_com_previsao = sorted(prev_map.keys())
    if not trimestres_com_previsao:
        return ano_ref, trimestres[-1]

    trimestre_detalhe = st.selectbox(
        "Ver detalhe da previsão por órgão para o trimestre:",
        options=trimestres_com_previsao,
        index=max(range(len(trimestres_com_previsao)), key=lambda i: cobertura(trimestres_com_previsao[i])),
        format_func=lambda t: (
            f"T{t} — {prev_map[t][3]}/{total_orgaos_universo} órgãos"
            + (" (já tem dado real também)" if real_map.get(t) else "")
        ),
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
