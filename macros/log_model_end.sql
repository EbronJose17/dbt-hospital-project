{% macro log_macro_end(model_name, invocation_id) %}

update {{target.database}}.audit_schema.dbt_model_control
set
    status = 'SUCCESS',
    run_ended_at = current_timestamp()   
where
    invocation_id = '{{invocation_id}}' and
    model_name = '{{model_name}}' and
    status like 'STARTED'

{% endmacro %}