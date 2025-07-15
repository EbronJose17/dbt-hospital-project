{% macro log_model_start(model_name, invocation_id, materialization, database, schema) %}

insert into {{database}}.audit_schema.dbt_model_control(
    model_name,
    invocation_id,
    run_started_at,
    status,
    materialization_type,
    target_database,
    target_schema
) values (
    '{{model_name}}',
    '{{invocation_id}}',
    current_timestamp(),
    'STARTED',
    '{{materialization}}',
    '{{database}}',
    '{{schema}}'
)

{% endmacro %}