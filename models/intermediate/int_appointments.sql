{{
    config(
        schema = "intermediate_schema"
    )
}}

select 
    row_number() over (order by(select null)) as appointment_sk,
    coalesce(appointment_id, 'Unknown') as appointment_id,
    coalesce(patient_id, 'Unknown') as patient_id,
    coalesce(doctor_id, 'Unknown') as doctor_id,
    to_date(appointment_date) as appointment_date,
    to_time(appointment_time) as appointment_time,
    coalesce(reason_for_visit, 'No Reason Provided') as reason_for_visit,
    coalesce(status, 'Unknown Status') as status
from 
    {{ref('stg_appointments')}}