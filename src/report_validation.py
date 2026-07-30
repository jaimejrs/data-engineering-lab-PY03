"""Verificação pós-geração dos relatórios narrativos (IA generativa) contra os
números realmente fornecidos ao LLM — achado da auditoria de rigor científico
de ML/DS de 30/07/2026: `models/narrative_report.py` e `streamlit/ai_report.py`
instruem o modelo a "não inventar números", mas rodam com `temperature=0.3`
(não 0) e não tinham NENHUM passo automatizado conferindo se o texto gerado
realmente bate com o que foi injetado no prompt. Um relatório que alimenta
decisão de gestor público não pode depender só da promessa do LLM de não
alucinar — precisa de um cinto de segurança verificável.

Heurística: extrai todo valor em R$ do texto gerado e confere se cada um
corresponde (com tolerância de arredondamento) a algum valor que estava
realmente disponível no prompt. Não é prova formal de ausência de alucinação
(o LLM pode legitimamente somar/agregar valores fornecidos em prosa, o que
geraria falso positivo aqui) — é um sinal de alerta bom o bastante pra pedir
revisão humana antes de confiar no relatório, que é o objetivo: nunca deixar
um número inventado passar em silêncio.

Duplicado (não importado) em `streamlit/ai_report.py`: o painel Streamlit tem
Docker/build context próprio (`streamlit/Dockerfile`, `COPY . .` só do
diretório `streamlit/`) e não tem acesso a `src/` — mesmo padrão já usado
nesse módulo pra outras funções de formatação (`streamlit/formatting.py` já
duplica lógica similar à daqui, não importa `src/`).
"""

import re

_RE_VALOR_REAIS = re.compile(r"R\$\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)")

# Diferença relativa tolerada entre um valor extraído do texto e o valor
# original — cobre arredondamento de exibição (ex: o LLM escrever "R$ 1.234,57"
# pra um valor original de 1234.567), não uma tolerância pra "quase certo".
TOLERANCIA_RELATIVA = 0.005


def _parse_valor_brl(texto: str) -> float:
    """'1.234.567,89' -> 1234567.89 (formato brasileiro: ponto de milhar, vírgula decimal)."""
    return float(texto.replace(".", "").replace(",", "."))


def extrair_valores_reais(texto: str) -> list[float]:
    """Todo valor em formato 'R$ 1.234,56' encontrado no texto, como float."""
    return [_parse_valor_brl(m) for m in _RE_VALOR_REAIS.findall(texto)]


def valores_nao_verificados(texto: str, valores_permitidos: set[float]) -> list[float]:
    """Valores em R$ do texto gerado que NÃO correspondem (dentro de
    `TOLERANCIA_RELATIVA`) a nenhum valor realmente fornecido ao LLM — sinal
    de possível alucinação numérica, a ser revisado por um humano antes de
    publicar/confiar no relatório."""
    encontrados = extrair_valores_reais(texto)
    return [
        v
        for v in encontrados
        if not any(
            abs(v - permitido) <= TOLERANCIA_RELATIVA * max(abs(permitido), 1.0) for permitido in valores_permitidos
        )
    ]
