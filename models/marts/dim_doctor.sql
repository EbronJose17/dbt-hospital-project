{{
    config(
        materialized = 'incremental',
        unique_key = 'doctor_id',
        schema = 'marts_schema',
    )
}}

with source as (
    select 
        doctor_sk,
        doctor_id,
        first_name,
        last_name,
        specialization,
        phone_number,
        years_experience,
        hospital_branch,
        email,
        _dbt_updated_at as updated_at
    from 
        {{ref('int_doctors')}}
)
{% if is_incremental() %}


,

target as (
    select 
        doctor_sk,
        doctor_id,
        first_name,
        last_name,
        specialization,
        phone_number,
        years_experience,
        hospital_branch,
        email,
        updated_at
    from 
        {{this}}
),

diff_data as (
    select 
        src.*
    from 
        source src left join target tgt 
    on 
        src.doctor_id = tgt.doctor_id
    where 
        tgt.doctor_id is null or
        src.doctor_sk <> tgt.doctor_sk or
        src.first_name <> tgt.first_name or 
        src.last_name <> tgt.last_name or
        src.specialization <> tgt.specialization or
        src.phone_number <> tgt.phone_number or 
        src.years_experience <> tgt.years_experience or 
        src.hospital_branch <> tgt.hospital_branch or
        src.email <> tgt.email
)

select * from diff_data

{% else %}

select * from source

{% endif %}