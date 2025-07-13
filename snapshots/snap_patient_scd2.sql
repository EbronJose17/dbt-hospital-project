{% snapshot snap_patient_scd2 %}

{{
    config(
        target_schema = 'snapshot_schema',
        unique_key = 'patient_id',
        strategy = 'check',
        check_cols = ['first_name', 'last_name', 'gender', 'date_of_birth', 'contact_number', 'address', 'registration_date', 'insurance_provider', 'insurance_number', 'email']
    )
}}

select 
    *
from    
    {{ref('int_patients')}}

{% endsnapshot %}