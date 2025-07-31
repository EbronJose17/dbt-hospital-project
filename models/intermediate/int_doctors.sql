{{
    config(
        schema = "intermediate_schema",
        materialized = "table",
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = "{{ log_macro_end(this.name, invocation_id, 'created_at') }}",
        tags ='doctor'
    )
}}

select 
    coalesce(doctor_id, 'Unknown') as doctor_id,
    coalesce(first_name, 'Unknown') as first_name,
    coalesce(last_name, 'Unknown') as last_name,
    coalesce(specialization, 'Unknown') as specialization,
    coalesce(phone_number, 'Unknown') as phone_number,
    coalesce(years_experience, 0) as years_experience,
    coalesce(hospital_branch, 'Unknown') as hospital_branch,
    coalesce(email, 'Unknown') as email,
    created_at
from    
    {{ref('stg_doctors')}}
where 
    created_at > to_timestamp_ntz('{{get_max_updated_timestamp(this.name)}}')
