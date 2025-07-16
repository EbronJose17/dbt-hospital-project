{{
    config(
        schema = "intermediate_schema",
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['treatment']
    )
}}

with min_max_cost as (
    select 
        treatment_type,
        description,
        min(cost) as min_treatment_cost,
        max(cost) as max_treatment_cost 
    from 
        {{ref('stg_treatments')}}
    group by    
        treatment_type,
        description
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
        concat(m.min_treatment_cost, ' - ', m.max_treatment_cost)as treatment_cost_range,
        current_timestamp() as _dbt_updated_at
    from
        base t
    left join 
        min_max_cost m
    on 
        t.treatment_type = m.treatment_type and
        t.description = m.description
) 

select * from final 
