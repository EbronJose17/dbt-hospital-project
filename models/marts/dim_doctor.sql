{{ config(
    materialized='incremental',
    unique_key='doctor_id',
    tags = ['dim', 'doctor'],
    pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
    post_hook = '{{ log_macro_end(this.name, invocation_id) }}'
)}}


select
    *
from    
    {{ref('int_doctors')}}
    
{% if is_incremental() %}
     where doctor_id not in (
        select t.doctor_id from {{this}} t
        join {{ref('int_doctors')}} s on t.doctor_id = s.doctor_id
        where 
            t.first_name <> s.first_name and
            t.last_name <> s.last_name and
            t.specialization <> s.specialization and 
            t.phone_number <> s.phone_number and 
            t.years_experience <> s.years_experience and 
            t.hospital_branch <> s.hospital_branch and
            t.email <> s.email
     )
{% endif %}