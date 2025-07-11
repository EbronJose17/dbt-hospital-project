{{
    config(
        schema = "intermediate_schema"
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
    coalesce(email, 'Unknown') as email
from    
    {{ref('stg_doctors')}}