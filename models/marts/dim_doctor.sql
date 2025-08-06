{{ config(
    materialized='incremental',
    unique_key='doctor_id',
    tags = ['dim', 'doctor'],
    pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
    post_hook = '{{ log_macro_end(this.name, invocation_id) }}'
)}}

{% if is_incremental() %}

with existing_records as (
    select
        *
    from 
        {{this}}
),

incoming_records as (
    select 
        *
    from 
        {{ref('int_doctors')}}
),

new_records as (
    select 
        *
    from 
        incoming_records
    where 
        doctor_id not in (select doctor_id from existing_records)
),

updated_records as (
    select 
        i.*
    from 
        existing_records e join incoming_records i
    on 
        e.doctor_id = i.doctor_id
    where
        e.first_name <> i.first_name or 
        e.last_name <> i.last_name or
        e.specialization <> i.specialization or 
        e.phone_number <> i.phone_number or 
        e.years_experience <> i.years_experience or 
        e.hospital_branch <> i.hospital_branch or 
        e.email <> i.email
)

select * from new_records
union all
select * from updated_records

{% else %}

select * from {{ref('int_doctors')}}

{% endif %}