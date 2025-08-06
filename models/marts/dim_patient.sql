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

with tgt as (
    select * from {{ this }} where is_current = 'Y'
),

source_snapshot as (
    select * from {{ ref('snap_patient_scd2') }}
),

newly_changed_patients as (
    select * from (
        select
            row_number() over (partition by patient_id order by dbt_valid_from desc) as rn,
            *
        from source_snapshot
    )
    where rn <= 2
),

f_insert as (
    select 
        src.dbt_scd_id as patient_sk,
        src.patient_id,
        src.first_name,
        src.last_name,
        src.gender,
        src.date_of_birth,
        src.contact_number,
        src.address,
        src.registration_date,
        src.insurance_provider,
        src.insurance_number,
        src.email,
        src.dbt_valid_from as effective_start_date,
        null::timestamp as effective_end_date,
        'Y'::varchar(1) as is_current
    from tgt
    left join newly_changed_patients src
        on src.patient_id = tgt.patient_id
    where src.rn = 1 
        and (
            tgt.patient_id is null 
            or tgt.effective_start_date <> src.dbt_valid_from
        )
),

f_update as (
    select 
        tgt.patient_sk,
        tgt.patient_id,
        tgt.first_name,
        tgt.last_name,
        tgt.gender,
        tgt.date_of_birth,
        tgt.contact_number,
        tgt.address,
        tgt.registration_date,
        tgt.insurance_provider,
        tgt.insurance_number,
        tgt.email,
        tgt.effective_start_date,
        src.dbt_valid_to as effective_end_date,
        'N'::varchar(1) as is_current
    from tgt
    left join newly_changed_patients src
        on src.patient_id = tgt.patient_id
    where src.rn = 2 
        and tgt.effective_start_date = src.dbt_valid_from
)

select * from f_insert
union all
select * from f_update
