{#
  Override do macro padrão do dbt. Por padrão, `{{ config(schema='audit') }}`
  concatenaria com o schema do profile (`gold_audit`, não `audit`) — aqui
  usa o schema custom exatamente como declarado, sem prefixo, permitindo
  schema físico por propósito (gold = modelo dimensional, ml = saída de
  modelo, audit = telemetria do próprio pipeline). Modelo sem `schema`
  custom cai no schema do profile (`gold`), como sempre.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
