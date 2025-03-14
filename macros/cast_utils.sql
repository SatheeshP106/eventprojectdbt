{% macro cast(column, data_type) %}
    CAST({{ column }} AS {{ data_type }})
{% endmacro %}