{{
    config(
        schema = "intermediate_schema",
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}'
    )
}}

select 
    concat('A', row_number() over (order by(appointment_id)))  as appointment_sk,
    coalesce(appointment_id, 'Unknown') as appointment_id,
    coalesce(patient_id, 'Unknown') as patient_id,
    coalesce(doctor_id, 'Unknown') as doctor_id,
    to_date(appointment_date) as appointment_date,
    to_time(appointment_time) as appointment_time,
    coalesce(reason_for_visit, 'No Reason Provided') as reason_for_visit,
    coalesce(status, 'Unknown Status') as status,
    current_timestamp() as _dbt_updated_at
from 
    {{ref('stg_appointments')}}