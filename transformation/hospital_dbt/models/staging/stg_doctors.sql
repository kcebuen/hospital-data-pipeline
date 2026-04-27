Select
    "doctor_id" as doctor_id,
    "first_name" as first_name,
    "last_name" as last_name,
    concat("first_name",' ',"last_name") as full_name,
    "specialization" as specialization,
    "phone_number" as phone_number,
    "years_experience" as years_experience,
    "hospital_branch" as hospital_branch,
    "email" as email
From {{ source('hospital_raw', 'doctors') }}