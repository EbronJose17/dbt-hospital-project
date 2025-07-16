{{
    config(
        materialized = 'table',
        schema = 'staging_schema',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['treatment']
    )
}}


select 
    * 
from 
    {{source('hospital_db', 'treatments')}}