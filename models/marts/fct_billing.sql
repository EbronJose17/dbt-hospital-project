{{
    config(
        schema = 'marts_schema',
        materialized = 'table'
    )
}}

select 
    billing.billing_sk as bill_key,
    patient.patient_sk as patient_key,
    treatment_type.treatment_type_sk as treatment_key,
    billing.bill_date,
    billing.amount,
    billing.payment_method,
    billing.payment_status
from 
    {{ref('int_billing')}} billing
left join 
    {{ref('dim_patient')}} patient 
on  
    billing.patient_id = patient.patient_id
left join 
    {{ref('int_treatments')}} treatments
on 
    billing.treatment_id = treatments.treatment_id
left join 
    {{ref('dim_treatment_type')}} treatment_type 
on  
    treatment_type.treatment_type = treatments.treatment_type