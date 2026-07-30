"""Formatação de valores e classificação de risco, compartilhadas entre as abas."""

import pandas as pd
from config import FAIXA_ALTO, FAIXA_BAIXO, FAIXA_MEDIO


def formatar_bilhoes(valor: float) -> str:
    """Formata um valor em R$ na casa de bilhões, ex: R$ 1,23 bi."""
    bi = (valor or 0) / 1_000_000_000
    texto = f"{bi:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto} bi"


def formatar_moeda_adaptativo(valor: float) -> str:
    """Formata em R$ escolhendo a casa (bi/mi/mil) pela ordem de grandeza —
    ao contrário de formatar_bilhoes(), que sempre mostra "bi" mesmo para
    valores pequenos (ex: um órgão com R$ 100 milhões apareceria como
    "R$ 0,10 bi", pouco legível). Usado no drill-down por órgão específico,
    onde o valor pode ser bem menor que o agregado do estado inteiro."""
    valor = valor or 0
    sinal = "-" if valor < 0 else ""
    absoluto = abs(valor)

    if absoluto >= 1_000_000_000:
        numero, sufixo = absoluto / 1_000_000_000, "bi"
    elif absoluto >= 1_000_000:
        numero, sufixo = absoluto / 1_000_000, "mi"
    elif absoluto >= 1_000:
        numero, sufixo = absoluto / 1_000, "mil"
    else:
        texto = f"{absoluto:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{sinal}R$ {texto}"

    texto = f"{numero:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{sinal}R$ {texto} {sufixo}"


def fmt_reais(valor) -> str:
    if valor is None or pd.isna(valor):
        return "não informado"
    texto = f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto}"


def classificar_risco(score: float) -> str:
    if score < 0.70:
        return FAIXA_BAIXO
    elif score <= 0.85:
        return FAIXA_MEDIO
    else:
        return FAIXA_ALTO


def escapar_cifrao_markdown(texto: str) -> str:
    """
    O st.markdown interpreta pares de '$' como delimitadores de fórmula
    LaTeX (KaTeX). Como o relatório tem vários "R$" no texto, isso faz
    trechos inteiros virarem "fórmulas" quebradas visualmente. Escapamos
    o cifrão para que apareça como texto literal.
    """
    return texto.replace("$", "\\$")
