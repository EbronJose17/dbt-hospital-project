{{ config(
    materialized = 'incremental',
    unique_key = 'doctor_id',
    merge_update_columns = ['first_name', 'last_name', 'specialization', 'phone_number', 'years_experience', 'hospital_branch', 'email'],
    tags = ['dim', 'doctor'],
    pre_hook = "{{ log_model_start(this.name, invocation_id, model.config.materialized, target.database, model.config.schema) }}",
    post_hook = '{{ log_macro_end(this.name, invocation_id) }}'
)}}


SELECT
    int_doctor.doctor_sk,
    int_doctor.doctor_id,
    int_doctor.first_name,
    int_doctor.last_name,
    int_doctor.specialization,
    int_doctor.phone_number,
    int_doctor.years_experience,
    int_doctor.hospital_branch,
    int_doctor.email,
    {% if is_incremental() %}
        COALESCE(target_doctor.created_at, int_doctor.created_at) AS created_at,
    {% else %}
        int_doctor.created_at AS created_at, 
    {% endif %}
    CURRENT_TIMESTAMP() AS updated_at
FROM
    {{ ref('int_doctors') }} int_doctor

{% if is_incremental() %}
LEFT JOIN {{ this }} AS target_doctor 
    ON int_doctor.doctor_id = target_doctor.doctor_id
{% endif %}