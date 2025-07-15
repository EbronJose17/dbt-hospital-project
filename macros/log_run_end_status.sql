{% macro log_run_end_status(database, invocation_id, results) %}

{% for result in results %}
    {% if result.status == 'error' or result.status == 'failed' %}
        update {{database}}.audit_schema.dbt_model_control
        set
            status = 'FAILED',
            run_ended_at = current_timestamp()
        where   
            invocation_id = '{{invocation_id}}' and
            status = 'STARTED'
    {% endif %}
{% endfor %}

{% endmacro %}