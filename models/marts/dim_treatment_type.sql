{{
    config(
        materialized = 'incremental',
        unique_key = 'treatment_type_sk',
        schema = 'marts_schema',
        merge_update_columns = ['treatment_type', 'treatment_description', 'treatment_cost_range'],
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

select 
    *,
    current_timestamp() as _dbt_updated_at 
from 
    final

-- {% if is_incremental() %}
--     where updated_at > (select max(updated_at) from {{ this }}) 
-- {% endif %}