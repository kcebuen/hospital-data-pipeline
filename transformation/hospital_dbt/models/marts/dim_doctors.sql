Select
    doctor_id,
    first_name,
    last_name,
    full_name,
    specialization,
    phone_number,
    years_experience,
    hospital_branch,
    email
From {{ ref('stg_doctors') }}