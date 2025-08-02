{{
    config(
        materialized = 'incremental',
        unique_key = 'treatment_type_sk',
        schema = 'marts_schema',
        pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
        post_hook = '{{ log_macro_end(this.name, invocation_id) }}',
        tags = ['dim', 'treatment']
    )
}}

with base as(
    select 
        row_number() over (partition by treatment_type, treatment_description order by treatment_type) as rn,
        *
    from 
        {{ref('int_treatments')}}
),

final as (
    select 
        {{dbt_utils.generate_surrogate_key(['treatment_type', 'treatment_description'])}} as treatment_type_sk,
        treatment_type,
        treatment_description,
        treatment_cost_range
    from 
        base 
    where rn = 1
)

select * from final