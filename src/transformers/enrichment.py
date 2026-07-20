"""
Enriquecimento: cruza empenhos/ordem_bancaria_orcamentaria com unidade_gestora.

Escopo (Fase 2, tarefa 17): junta os registros de despesa com os dados do
órgão gestor correspondente. Join validado no README/dicionario-dados.md:
`(codigoug, ano)` em empenhos/OB casa com `(codigo, ano)` em unidade_gestora
(unidade_gestora é versionada por ano — nunca juntar só por `codigo`).
"""

import pandas as pd

UNIDADE_GESTORA_COLUMNS = {
    "codigo", "ano", "titulo", "sigla", "cnpj", "tipoadministracao",
    "tipoug", "codigopoder", "nomepoder", "codigouf", "nomemunicipio",
}


def enrich_with_unidade_gestora(records: pd.DataFrame, unidade_gestora: pd.DataFrame) -> pd.DataFrame:
    """Junta `records` (empenhos ou ordem_bancaria_orcamentaria) com `unidade_gestora`.

    Left join por `(codigoug, ano)` == `(codigo, ano)` -- preserva todo registro
    de despesa mesmo sem órgão correspondente (não descarta dado por join fraco).
    """
    if records.empty:
        return records

    ug = unidade_gestora[[c for c in UNIDADE_GESTORA_COLUMNS if c in unidade_gestora.columns]].copy()
    ug = ug.rename(columns={"codigo": "codigoug", "titulo": "orgao_nome", "sigla": "orgao_sigla",
                             "cnpj": "orgao_cnpj", "tipoadministracao": "orgao_tipo_administracao",
                             "tipoug": "orgao_tipo_ug", "codigopoder": "orgao_codigo_poder",
                             "nomepoder": "orgao_nome_poder", "codigouf": "orgao_codigo_uf",
                             "nomemunicipio": "orgao_nome_municipio"})

    return records.merge(ug, on=["codigoug", "ano"], how="left")
