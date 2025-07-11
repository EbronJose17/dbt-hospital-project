{{
    config(
        materialized = 'table',
        schema = 'staging_schema'
    )
}}


select 
    *
from 
    {{source('hospital_db', 'patients')}}