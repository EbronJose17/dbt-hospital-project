{{
    config(
        materialized = 'incremental',
        unique_key = 'doctor_id',
        schema = 'marts_schema',
        merge_update_columns = ['specialization', 'phone_number', 'years_experience', 'hospital_branch', 'email'],
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['dim', 'doctor']
    )
}}

select 
    doctor_sk,
    doctor_id,
    first_name,
    last_name,
    specialization,
    phone_number,
    years_experience,
    hospital_branch,
    email,
    current_timestamp as updated_at
from 
    {{ref('int_doctors')}}