"""Componente de IA generativa do painel — relatório narrativo sob demanda.

Mesmo padrão do models/narrative_report.py do pipeline principal: o LLM só
reescreve/traduz números já calculados, nunca recebe dado bruto nem infere
valor novo (evita alucinação).

Verificação pós-geração (auditoria de rigor científico, 30/07/2026): o prompt
já instrui "não invente números", mas a chamada roda com `temperature=0.3`
(não 0) e não havia nenhum passo automatizado conferindo se o texto gerado
bate com os números realmente fornecidos. `_valores_nao_verificados()` extrai
todo valor em R$ do texto e confere contra o conjunto de valores injetados no
prompt — duplicado (não importado) de `src/report_validation.py`, que tem a
mesma lógica pro pipeline principal: este painel tem build context Docker
próprio (`streamlit/Dockerfile`, `COPY . .` só do diretório `streamlit/`) e
não enxerga `src/`.
"""

import os
import re

import pandas as pd
from formatting import fmt_reais
from openai import OpenAI

import streamlit as st

OPENAI_MODEL_DEFAULT = "gpt-4o-mini"

_RE_VALOR_REAIS = re.compile(r"R\$\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)")
_TOLERANCIA_RELATIVA = 0.005


def _parse_valor_brl(texto: str) -> float:
    return float(texto.replace(".", "").replace(",", "."))


def _valores_nao_verificados(texto: str, valores_permitidos: set[float]) -> list[float]:
    """Valores em R$ do texto gerado que não correspondem (dentro de
    `_TOLERANCIA_RELATIVA`) a nenhum valor realmente fornecido ao LLM — ver
    docstring do módulo."""
    encontrados = [_parse_valor_brl(m) for m in _RE_VALOR_REAIS.findall(texto)]
    return [
        v
        for v in encontrados
        if not any(abs(v - p) <= _TOLERANCIA_RELATIVA * max(abs(p), 1.0) for p in valores_permitidos)
    ]


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
                f"valor: {fmt_reais(r['valor_contrato'])}, grau de atipicidade: {float(r['score_anomalia']):.0%}"
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
                f"{int(r['ano_previsto'])}: previsão central {fmt_reais(r['valor_previsto_p50'])} "
                f"(intervalo entre {fmt_reais(r['valor_previsto_p10'])} e {fmt_reais(r['valor_previsto_p90'])})"
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


def _valores_permitidos(anomalias: pd.DataFrame, previsoes: pd.DataFrame) -> set[float]:
    """Todo valor numérico realmente injetado no prompt (ver `build_prompt`)."""
    permitidos: set[float] = set()
    if not anomalias.empty:
        permitidos.update(float(v) for v in anomalias["valor_contrato"].dropna())
    if not previsoes.empty:
        for coluna in ("valor_previsto_p10", "valor_previsto_p50", "valor_previsto_p90"):
            permitidos.update(float(v) for v in previsoes[coluna].dropna())
    return permitidos


def gerar_relatorio_ia(anomalias: pd.DataFrame, previsoes: pd.DataFrame) -> tuple[str, list[float]]:
    """Retorna `(texto_markdown, valores_suspeitos)` — `valores_suspeitos` são
    valores em R$ do texto gerado que não bateram com nenhum número fornecido
    ao LLM (ver docstring do módulo); vazio quando nada suspeito foi encontrado."""
    system_prompt, user_prompt = build_prompt(anomalias, previsoes)
    client = get_openai_client()
    model = os.getenv("OPENAI_MODEL", OPENAI_MODEL_DEFAULT)

    resposta = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
    )
    texto = resposta.choices[0].message.content
    suspeitos = _valores_nao_verificados(texto, _valores_permitidos(anomalias, previsoes))
    return texto, suspeitos
