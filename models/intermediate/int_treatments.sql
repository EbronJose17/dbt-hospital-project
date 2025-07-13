{{
    config(
        schema = "intermediate_schema"
    )
}}

select
    {{dbt_utils.generate_surrogate_key(['treatment_id']) }} as treatment_sk,
    coalesce(treatment_id, 'Unknown') as treatment_id,
    coalesce(appointment_id, 'Unknown') as appointment_id,
    {{dbt_utils.generate_surrogate_key(['treatment_type'])}} as treatment_type_sk,
    coalesce(treatment_type, 'Unknown') as treatment_type,
    coalesce(description, 'Unknown') as description,
    cast(cost as numeric(10, 2)) as cost,
    to_date(treatment_date) as treatment_date
from
    {{ref('stg_treatments')}}