{{
    config(
        schema = 'marts_schema',
        materialized = 'table'
    )
}}

select 
    *
from
    {{ ref('int_appointments') }} appointments 
left join 
    {{ref('dim_patient')}} patients 
on
    appointments.patient_id = patients.patient_id
left join 
    {{ref('dim_doctor')}} doctor
on
    appointments.doctor_id = doctor.doctor_id