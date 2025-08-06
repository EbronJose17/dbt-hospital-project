{% macro test_control(model_name, max_timestamp)%}

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

update hospital_db.audit_schema.test_control_table set
start_param = (select end_param from hospital_db.audit_schema.test_control_table where model_name = '{{model_name}}'),
end_param = '{{calculated_max_timestamp}}'
where model_name = '{{model_name}}'
{% endmacro %}