{% macro get_max_updated_timestamp(model_name) %}

{% set query %}
select
    max(max_timestamp)
from
    hospital_db.audit_schema.dbt_model_control
where
    model_name = '{{model_name}}' 
{% endset %}

{% set result_set = run_query(query) %} 

{% if execute %} 
    {% set default_safe_timestamp_str = '1900-01-01 00:00:00' %}

    {% if result_set and result_set.columns[0].values() %}

        {% set raw_value = result_set.columns[0].values() | first %}

        {% if raw_value is not none %}
            {{ return(raw_value) }}
        {% else %}
            {{ return(default_safe_timestamp_str) }}
        {% endif %}
    {% else %}
        {{ return(default_safe_timestamp_str) }}
    {% endif %}
{% else %}
    {{ return('1900-01-01 00:00:00') }}
{% endif %}

{% endmacro %}