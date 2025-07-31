{% macro log_macro_end(model_name, invocation_id, max_timestamp=none) %}

{% set calculated_max_timestamp = none %}

{% if execute and max_timestamp is not none%}

    {%- set latest_ts_query -%}
        SELECT MAX({{ max_timestamp }}) FROM {{ this }}
    {%- endset -%}

    {% set query_result  = run_query(latest_ts_query) %}

    {% if query_result and query_result.columns[0].values() %}
        {% set calculated_max_timestamp = query_result.columns[0].values() | first %}
    {% endif %}

{% endif %}


update {{target.database}}.audit_schema.dbt_model_control
set
    status = 'SUCCESS',
    run_ended_at = current_timestamp(),
    max_timestamp = 
        {% if calculated_max_timestamp is not none %}
            '{{ calculated_max_timestamp}}'
        {% else %}
            NULL
        {% endif %}
where
    invocation_id = '{{invocation_id}}' and
    model_name = '{{model_name}}' and
    status like 'STARTED'

{% endmacro %}