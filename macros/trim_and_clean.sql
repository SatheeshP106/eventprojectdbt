{% macro trim_and_clean(field) %}
    TRIM(LOWER({{ field }}))
{% endmacro %}