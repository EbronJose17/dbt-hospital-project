{{
    config(
        materialized = 'table',
        schema = 'marts_schema',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['dim', 'patient']
    )
}}

select 
    concat ('P', row_number() over (order by(select null))) as patient_sk,
    patient_id,
    first_name,
    last_name,
    gender,
    date_of_birth,
    contact_number,
    address,
    registration_date,
    insurance_provider,
    insurance_number,
    email,
    dbt_valid_from as effective_start_date,
    coalesce(dbt_valid_to, '9999-12-31') as effective_end_date,
    case
        when dbt_valid_to is null 
        then 'Y' else 'N'
    end as is_current
from 
    {{ref('snap_patient_scd2')}}