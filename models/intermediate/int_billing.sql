{{
    config(
        schema = "intermediate_schema"
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
    coalesce(payment_status, 'Unknown') as payment_status 
from 
    {{ref('stg_billing')}}