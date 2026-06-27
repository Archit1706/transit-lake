{# Build a deterministic surrogate key by hashing the given columns.
   Local replacement for dbt_utils.generate_surrogate_key so the project has no
   external package dependency. #}
{% macro surrogate_key(columns) %}
    md5(
{%- for col in columns %}
        coalesce(cast({{ col }} as varchar), '_null_')
{%- if not loop.last %} || '||' || {% endif %}
{%- endfor %}
    )
{% endmacro %}
