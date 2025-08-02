{{
    config(
        schema = 'marts_schema',
        materialized = 'table',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['fact', 'patient_activity', 'summary']
    )
}}

with patient_appointments as (
    select 
        patient_id,
        count(*) as total_appointments,
        count(case when status like 'No-show' then 1 end) as no_show_count,
        min(appointment_date) as first_appointment_date,
        max(appointment_date) as last_appointment_date
    from 
        {{ref('fct_appointments')}}
    group by
        patient_id
),

patient_billing as (
    select 
        patient_id,
        sum(amount) as total_billed,
        sum(case when payment_status = 'Paid' then amount end) as total_paid
    from 
        {{ref("fct_billing")}}
    group by 
        patient_id
),

final as (
    select
        patient.patient_id as patient_id,
        coalesce(patient_appointments.total_appointments, 0) as total_appointments,
        patient_billing.total_billed,
        coalesce(patient_billing.total_paid, 0) as total_paid,
        coalesce(patient_appointments.no_show_count, 0) as no_show_count,
        patient_appointments.first_appointment_date,
        patient_appointments.last_appointment_date,
        patient.effective_start_date
    from    
        {{ref('dim_patient')}} patient
    left join 
        patient_appointments
    on
        patient_appointments.patient_id = patient.patient_id
    left join 
        patient_billing
    on 
        patient_billing.patient_id = patient_appointments.patient_id
    where 
        patient.is_current = 'Y'
    order by patient.patient_id
)   



select * from final