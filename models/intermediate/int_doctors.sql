{{
    config(
        schema = "intermediate_schema",
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags ='doctor'
    )
}}

select 
    concat('D', row_number() over(order by(select null))) as doctor_sk,
    coalesce(doctor_id, 'Unknown') as doctor_id,
    coalesce(first_name, 'Unknown') as first_name,
    coalesce(last_name, 'Unknown') as last_name,
    coalesce(specialization, 'Unknown') as specialization,
    coalesce(phone_number, 'Unknown') as phone_number,
    coalesce(years_experience, 0) as years_experience,
    coalesce(hospital_branch, 'Unknown') as hospital_branch,
    coalesce(email, 'Unknown') as email,
    current_timestamp() as _dbt_updated_at
from    
    {{ref('stg_doctors')}}