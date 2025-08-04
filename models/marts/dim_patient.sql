{{
    config(
        materialized = 'incremental',
        unique_key = 'patient_sk',
        schema = 'marts_schema',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['dim', 'patient']
    )
}}

with source_snapshot as (
    select 
        *
    from    
        {{ref('snap_patient_scd2')}}
),

newly_changed_patients as (
    select * from (
        select
            row_number() over (partition by patient_id order by dbt_valid_from desc) as rn,
            *
        from 
            source_snapshot
    )
    where rn <= 2
),

f_insert as (
    select 
        dbt_scd_id as patient_sk,
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
        newly_changed_patients where rn = 1
),

f_update as (
    select 
        dbt_scd_id as patient_sk,
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
        newly_changed_patients where rn = 2
)

select * from f_insert
union all
select * from f_update
