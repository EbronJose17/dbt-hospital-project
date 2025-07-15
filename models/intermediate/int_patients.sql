{{
    config(
        schema = "intermediate_schema",
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}'
    )
}}


select 
    concat ('P', row_number() over (order by(select null))) as patient_sk,
    coalesce(patient_id, 'Unknown') as patient_id,
    coalesce(first_name, 'Unknown') as first_name,
    coalesce(last_name, 'Unknown') as last_name,
    coalesce(gender, 'Unknown') as gender,
    to_date(date_of_birth) as date_of_birth,
    coalesce(contact_number, 'Unknown') as contact_number,
    coalesce(address, 'Unknown') as address,
    to_date(registration_date) as registration_date,
    coalesce(insurance_provider, 'Unknown') as insurance_provider,
    coalesce(insurance_number, 'Unknown') as insurance_number,
    coalesce(email, 'Unknown') as email,
    current_timestamp as updated_at
from 
    {{ref('stg_patients')}}