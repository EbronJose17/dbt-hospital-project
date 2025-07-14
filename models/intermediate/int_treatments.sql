{{
    config(
        schema = "intermediate_schema"
    )
}}

with avg_cost as (
    select avg(cost) as avg_treatment_cost from {{ref('stg_treatments')}}
),

base as (
    select
        *
    from 
        {{ref('stg_treatments')}}
),

final as (
    select
        concat('T', row_number() over(order by(treatment_id))) as treatment_sk,
        coalesce(t.treatment_id, 'Unknown') as treatment_id,
        coalesce(t.appointment_id, 'Unknown') as appointment_id,
        coalesce(t.treatment_type, 'Unknown') as treatment_type,
        coalesce(t.description, 'Unknown') as treatment_description,
        cast(t.cost as numeric(10, 2)) as treatment_cost,
        to_date(t.treatment_date) as treatment_date,
        case 
            when treatment_cost < a.avg_treatment_cost then 'Low'
            when treatment_cost > a.avg_treatment_cost then 'High'
        end as treatment_cost_range,
        current_timestamp() as updated_at
    from
        base t
    cross join 
        avg_cost a
) 

select * from final 
