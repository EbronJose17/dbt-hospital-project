{{
    config(
        schema = 'marts_schema',
        materialized = 'incremental',
        unique_key = 'appointment_id',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['fact', 'appointment']
    )
}}

select 
    appointments.appointment_id as appointment_id,
    patients.patient_id as patient_id,
    doctor.doctor_id as doctor_id,
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
where
    patients.is_current = 'Y'