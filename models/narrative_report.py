"""Componente de IA Generativa — relatório narrativo automático (tarefa 25).

Lê os outputs já gravados pelos Modelos 1 e 2, schema `ml`
(`iceberg.ml.score_anomalia_contrato`, `iceberg.ml.previsao_pagamento_orgao`)
via Trino e usa um LLM (API OpenAI, modelo de baixo custo) para escrever um
relatório narrativo em linguagem acessível — para um gestor público sem
formação técnica, não para um engenheiro de dados. Não treina nada: só lê o
que os dois modelos já escoraram/preveram e traduz em texto.

O relatório é gravado em dois lugares:
  - `iceberg.ml.relatorio_narrativo` (auditoria/histórico, mesmo padrão de
    `write_scores`/`write_forecasts` dos outros dois modelos — substitui a
    linha anterior a cada execução, não é incremental).
  - um arquivo Markdown em `NARRATIVE_REPORT_DIR` (default
    `models/artifacts/relatorios/`, já coberto por `models/artifacts/` no
    `.gitignore`) — para abrir e ler direto, sem precisar consultar o Trino.

Uso: python -m models.narrative_report [--top-anomalias 10] [--top-previsoes 10]
"""

import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

from src import trino_io

load_dotenv()

logger = logging.getLogger(__name__)

REPORT_TABLE = "iceberg.ml.relatorio_narrativo"
MODEL_VERSION = "narrative_report_v1"

# Modelo de baixo custo — resumir dado tabular já pronto em texto não exige o
# modelo "topo de linha" da OpenAI; gpt-4o-mini é a opção de custo/qualidade
# recomendada para esse tipo de tarefa. Configurável via env sem redeploy de código.
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

REPORT_DIR = os.environ.get(
    "NARRATIVE_REPORT_DIR",
    os.path.join(os.path.dirname(__file__), "artifacts", "relatorios"),
)

TOP_ANOMALIAS_QUERY = """
SELECT
    s.id_contrato_origem,
    s.ano,
    s.score_anomalia,
    fc.valor_contrato,
    dorg.nome AS orgao,
    dc.nome AS credor,
    dm.descricao_modalidade AS modalidade,
    fc.flag_emergency
FROM iceberg.ml.score_anomalia_contrato s
JOIN iceberg.gold.fato_contrato fc
    ON fc.id_contrato_origem = s.id_contrato_origem AND fc.ano = s.ano
LEFT JOIN iceberg.gold.dim_orgao dorg ON fc.sk_orgao = dorg.sk_orgao
LEFT JOIN iceberg.gold.dim_credor dc ON fc.sk_credor = dc.sk_credor
LEFT JOIN iceberg.gold.dim_modalidade dm ON fc.sk_modalidade = dm.sk_modalidade
ORDER BY s.score_anomalia DESC
LIMIT {top_n}
"""

TOP_PREVISOES_QUERY = """
SELECT
    codigo_orgao,
    nome_orgao,
    ano_previsto,
    trimestre_previsto,
    valor_previsto_p10,
    valor_previsto_p50,
    valor_previsto_p90
FROM iceberg.ml.previsao_pagamento_orgao
ORDER BY valor_previsto_p50 DESC
LIMIT {top_n}
"""


def extract_top_anomalias(top_n: int = 10) -> pd.DataFrame:
    return trino_io.query(TOP_ANOMALIAS_QUERY.format(top_n=int(top_n)))


def extract_top_previsoes(top_n: int = 10) -> pd.DataFrame:
    return trino_io.query(TOP_PREVISOES_QUERY.format(top_n=int(top_n)))


def _fmt_reais(valor: Any) -> str:
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return "R$ 0,00"


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


def generate_narrative(
    anomalias: pd.DataFrame, previsoes: pd.DataFrame, client: OpenAI | None = None, model: str = OPENAI_MODEL
) -> str:
    """Chama o LLM e devolve o relatório em Markdown. `client` é injetável para
    teste (evita chamada real à API OpenAI nos testes automatizados)."""
    client = client or OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    system_prompt, user_prompt = build_prompt(anomalias, previsoes)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
    )
    return response.choices[0].message.content


def save_report_file(conteudo: str, gerado_em: datetime, report_dir: str = REPORT_DIR) -> str:
    os.makedirs(report_dir, exist_ok=True)
    path = os.path.join(report_dir, f"relatorio_{gerado_em.strftime('%Y%m%d_%H%M%S')}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(conteudo)
    logger.info("Relatório salvo em %s", path)
    return path


def write_report_metadata(
    conteudo: str,
    gerado_em: datetime,
    num_anomalias: int,
    num_previsoes: int,
    llm_model: str,
    model_version: str = MODEL_VERSION,
) -> None:
    """Grava o relatório (texto completo + metadados) em `REPORT_TABLE` — mesmo
    padrão de `replace_table` usado por `write_scores`/`write_forecasts`."""
    payload = pd.DataFrame(
        [
            {
                "gerado_em": gerado_em.strftime("%Y-%m-%d %H:%M:%S"),
                "llm_model": llm_model,
                "model_version": model_version,
                "num_contratos_anomalos": int(num_anomalias),
                "num_orgaos_previstos": int(num_previsoes),
                "conteudo_markdown": conteudo,
            }
        ]
    )
    trino_io.replace_table(
        table=REPORT_TABLE,
        df=payload,
        columns=list(payload.columns),
        ddl=f"""
            CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
                gerado_em timestamp,
                llm_model varchar,
                model_version varchar,
                num_contratos_anomalos integer,
                num_orgaos_previstos integer,
                conteudo_markdown varchar
            )
        """,
        casts={"gerado_em": "TIMESTAMP"},
    )
    logger.info("Metadados do relatório gravados em %s", REPORT_TABLE)


def run(top_anomalias: int = 10, top_previsoes: int = 10, persist: bool = True) -> str:
    anomalias = extract_top_anomalias(top_anomalias)
    previsoes = extract_top_previsoes(top_previsoes)

    conteudo = generate_narrative(anomalias, previsoes)
    gerado_em = datetime.now(timezone.utc)
    save_report_file(conteudo, gerado_em)

    logger.info(
        "Relatório narrativo gerado: %s contratos atípicos, %s previsões de órgão",
        len(anomalias),
        len(previsoes),
    )
    if persist:
        write_report_metadata(conteudo, gerado_em, len(anomalias), len(previsoes), OPENAI_MODEL)
    return conteudo


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Componente de IA Generativa — relatório narrativo (tarefa 25)")
    parser.add_argument("--top-anomalias", type=int, default=10, help="Quantos contratos atípicos incluir no relatório")
    parser.add_argument("--top-previsoes", type=int, default=10, help="Quantas previsões de órgão incluir no relatório")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    texto = run(top_anomalias=args.top_anomalias, top_previsoes=args.top_previsoes)
    print(texto)
