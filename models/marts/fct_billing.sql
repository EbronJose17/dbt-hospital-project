{{
    config(
        schema = 'marts_schema',
        materialized = 'incremental',
        unique_key = 'bill_id',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['fact', 'billing']
    )
}}

select 
    billing.bill_id,
    patient.patient_id as patient_id,
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
    treatment_type.treatment_type = treatments.treatment_type and
    treatment_type.treatment_description = treatments.treatment_description
where
    patient.is_current = 'Y'