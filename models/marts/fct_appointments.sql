{{
    config(
        schema = 'marts_schema',
        materialized = 'table',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['fact', 'appointment']
    )
}}

select 
    concat('AP_',substring(appointments.appointment_sk, 2), substring(patients.patient_sk, 2), substring(doctor.doctor_sk, 2)) as appointment_sk,
    appointments.appointment_sk as appointment_key,
    patients.patient_sk as patient_key,
    doctor.doctor_sk as doctor_key,
    appointments.appointment_date,
    appointments.appointment_time,
    appointments.reason_for_visit,
    appointments.status
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