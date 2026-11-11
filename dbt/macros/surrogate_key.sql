{# Deterministic BIGINT surrogate key from one or more columns (xxhash64). #}
{% macro surrogate_key(columns) -%}
    from_big_endian_64(xxhash64(to_utf8(concat_ws('|', {% for c in columns %}coalesce(cast({{ c }} as varchar), '~'){% if not loop.last %}, {% endif %}{% endfor %}))))
{%- endmacro %}
