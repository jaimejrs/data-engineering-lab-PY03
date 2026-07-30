"""Fragmentos SQL dos filtros globais da sidebar (ano/órgão)."""


def anos_filter_sql(anos_selecionados: list, column: str) -> str:
    if not anos_selecionados:
        return ""
    anos_str = ", ".join(str(a) for a in anos_selecionados)
    return f"AND {column} IN ({anos_str})"


def orgaos_filter_sql(orgaos_selecionados: list, column: str) -> str:
    if not orgaos_selecionados:
        return ""
    nomes = ", ".join("'" + o.replace("'", "''") + "'" for o in orgaos_selecionados)
    return f"AND {column} IN ({nomes})"
