{{
    config(
        materialized = 'incremental',
        unique_key = 'doctor_id',
        schema = 'marts_schema',
        merge_update_columns = [],
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['dim', 'doctor']
    )
}}

select 
    *,
    current_timestamp as updated_at
from 
    {{ref('int_doctors')}}