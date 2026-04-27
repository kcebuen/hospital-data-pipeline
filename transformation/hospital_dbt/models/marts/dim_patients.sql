Select
    patient_id,
    first_name,
    last_name,
    full_name,
    gender,
    age,
    age_group,
    date_of_birth,
    contact_number,
    address,
    registration_date,
    insurance_provider,
    insurance_number,
    email
From {{ ref('stg_patients') }}