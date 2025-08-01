{{
    config(
        schema = "intermediate_schema",
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['billing']
    )
}}

select 
    concat('B', row_number() over (order by(select null))) as billing_sk,
    coalesce(bill_id, 'Unknown') as bill_id,
    coalesce(patient_id, 'Unknown') as patient_id,
    coalesce(treatment_id, 'Unknown') as treatment_id,
    to_date(bill_date) as bill_date,
    cast(amount as numeric(10, 2)) as amount,
    coalesce(payment_method, 'Unknown') as payment_method,
    coalesce(payment_status, 'Unknown') as payment_status,
    current_timestamp() as _dbt_updated_at
from 
    {{ref('stg_billing')}}