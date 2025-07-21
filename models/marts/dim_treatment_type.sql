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
        concat('T_TYPE_', row_number() over(order by(select null))) as treatment_type_sk,
        treatment_type,
        treatment_description,
        treatment_cost_range
    from 
        base 
    where rn = 1
)

{% if is_incremental() %}

, existing as (
    select 
        treatment_type_sk,
        treatment_type,
        treatment_description,
        treatment_cost_range,
        created_at 
    from 
        {{this}}
),

changed_records as (
    select 
        f.treatment_type_sk,
        f.treatment_type,
        f.treatment_description,
        f.treatment_cost_range,
        coalesce(e.created_at, current_timestamp()) as created_at,
        current_timestamp() as _dbt_updated_at
    from 
        final f left join existing e 
    on 
        f.treatment_type_sk = e.treatment_type_sk
    where 
        e.treatment_type_sk is null or 
        e.treatment_type <> f.treatment_type or
        e.treatment_description <> f.treatment_description or 
        e.treatment_cost_range <> f.treatment_cost_range
)

select 
    *
from 
    changed_records

{% else %}

select 
    *,
    current_timestamp() as created_at,
    current_timestamp() as _dbt_updated_at
from 
    final 

{% endif %}

