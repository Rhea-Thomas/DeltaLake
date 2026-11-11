{# Use "<target>_<custom>" (gold_staging) for custom schemas, "<target>" otherwise. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}
    {%- else -%}{{ target.schema }}_{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
