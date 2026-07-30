"""Aba "Resumo (IA)" — relatório narrativo gerado sob demanda."""

from datetime import datetime

from ai_report import gerar_relatorio_ia
from db import run_query
from formatting import escapar_cifrao_markdown
from pdf_export import gerar_pdf_relatorio
from sql_filters import anos_filter_sql, orgaos_filter_sql

import streamlit as st


def render(
    score_threshold: float,
    ano_ref: int,
    trimestre_previsto_sel: int,
    anos_selecionados: list,
    orgaos_selecionados: list,
) -> None:
    st.subheader(
        "Relatório narrativo (gerado por IA)",
        help=(
            "Os contratos com maior grau de atipicidade usam os filtros de Ano/Órgão da barra "
            "lateral. As previsões de pagamento usam sempre a referência escolhida na aba "
            "'Previsão de Pagamentos', independente desses filtros — são duas fontes com "
            "escopos próprios, combinadas no mesmo relatório."
        ),
    )
    st.caption(
        "Ao clicar no botão, os contratos com maior grau de atipicidade e as "
        "previsões de pagamento mais recentes são enviados como texto já "
        "calculado para o modelo de linguagem, que apenas reescreve os "
        "achados em português claro — nenhum valor é inventado pela IA."
    )
    st.caption(
        f"Referência de previsão usada: {ano_ref}-T{trimestre_previsto_sel} (ajustável na aba 'Previsão de Pagamentos')."
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
            LEFT JOIN iceberg.gold.dim_credor dcred ON fc.sk_credor = dcred.sk_credor
            JOIN iceberg.gold.dim_modalidade dmod ON fc.sk_modalidade = dmod.sk_modalidade
            WHERE fc.score_anomalia >= {score_threshold}
            {anos_filter_sql(anos_selecionados, "fc.ano")}
            {orgaos_filter_sql(orgaos_selecionados, "dorg.nome")}
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
                texto, valores_suspeitos = gerar_relatorio_ia(df_anom_ia, df_prev_ia)
                st.session_state["relatorio_ia_texto"] = texto
                st.session_state["relatorio_ia_valores_suspeitos"] = valores_suspeitos
                filtro_txt = []
                if anos_selecionados:
                    filtro_txt.append("ano " + ", ".join(str(a) for a in anos_selecionados))
                if orgaos_selecionados:
                    filtro_txt.append(f"{len(orgaos_selecionados)} órgão(s) selecionado(s)")
                filtro_meta = f" · filtro: {' · '.join(filtro_txt)}" if filtro_txt else " · sem filtro de ano/órgão"
                st.session_state["relatorio_ia_meta"] = (
                    f"{len(df_anom_ia)} contratos anômalos{filtro_meta} · "
                    f"{df_prev_ia['nome_orgao'].nunique() if not df_prev_ia.empty else 0} "
                    f"órgãos previstos · referência {ano_ref}-T{trimestre_previsto_sel}"
                )
            except Exception as e:
                st.error(f"Erro ao gerar relatório: {e}")

    if "relatorio_ia_texto" in st.session_state:
        st.divider()
        meta_txt = st.session_state.get("relatorio_ia_meta", "")
        st.caption(meta_txt)
        valores_suspeitos = st.session_state.get("relatorio_ia_valores_suspeitos", [])
        alerta_txt = ""
        if valores_suspeitos:
            valores_fmt = ", ".join(
                f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") for v in valores_suspeitos
            )
            alerta_txt = (
                f"{len(valores_suspeitos)} valor(es) neste relatório não batem com nenhum número "
                f"calculado pelos modelos: {valores_fmt}. Confira antes de usar — pode ser um erro "
                "de arredondamento do texto ou um valor que a IA calculou por conta própria."
            )
            st.warning(f"⚠️ {alerta_txt}")

        pdf_bytes = gerar_pdf_relatorio(
            st.session_state["relatorio_ia_texto"],
            titulo="Relatório Narrativo — Empenhos CE",
            meta=meta_txt,
            alerta=alerta_txt,
        )
        st.download_button(
            "📄 Salvar em PDF",
            data=pdf_bytes,
            file_name=f"relatorio_ia_{datetime.now():%Y%m%d_%H%M}.pdf",
            mime="application/pdf",
        )
        st.markdown(escapar_cifrao_markdown(st.session_state["relatorio_ia_texto"]))
