{#
  Surrogate key determinística por hash (md5 hex) da chave natural.
  Iceberg não tem sequence (BIGSERIAL); o hash é estável entre execuções e
  entre dim/fato, então o mesmo `sk(...)` na dimensão e no fato casa o join.
  Sem dbt_utils (evita `dbt deps`/egress) — usa só funções nativas do Trino.
  NULLs viram '' antes do hash; separador '||' evita colisão entre colunas.
#}
{% macro sk(columns) %}
    lower(to_hex(md5(to_utf8(
        {%- for c in columns %}
        coalesce(cast({{ c }} as varchar), '')
        {%- if not loop.last %} || '||' || {% endif -%}
        {%- endfor %}
    ))))
{% endmacro %}
