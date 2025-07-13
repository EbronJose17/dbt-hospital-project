{{
    config(
        materialized = 'incremental',
        unique_key = 'doctor_id',
        schema = 'marts_schema'
    )
}}

select 
    *
from 
    {{ref('int_doctors')}}