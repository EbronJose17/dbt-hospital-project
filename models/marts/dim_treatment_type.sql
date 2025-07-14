{{
    config(
        materialized = 'incremental',
        unique_key = 'treatment_type_sk',
        schema = 'marts_schema'
    )
}}

with base as(
    select 
        row_number() over (partition by treatment_type order by treatment_type) as rn,
        *
    from 
        {{ref('int_treatments')}}
),

final as (
    select 
        concat('T_TYPE_', row_number() over(order by(select null))) as treatment_type_sk,
        treatment_type,
        treatment_description,
        treatment_cost_range,
        updated_at
    from 
        base 
    where rn = 1
)

select * from final

{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }}) 
{% endif %}