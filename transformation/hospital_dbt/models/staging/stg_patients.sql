Select
    "patient_id" as patient_id,
    "first_name" as first_name,
    "last_name" as last_name,
    concat("first_name",' ',"last_name") as full_name,
    "gender" as gender,
    datediff(YEAR, "date_of_birth",current_date) as age,
    "date_of_birth" as date_of_birth,
    "contact_number" as contact_number,
    "address" as address,
    "registration_date" as registration_date,
    "insurance_provider" as insurance_provider,
    "insurance_number" as insurance_number,
    "email" as email,
    case
        when datediff(YEAR, "date_of_birth", current_date) <= 17 then '0-17'
        when datediff(YEAR, "date_of_birth", current_date) <= 30 then '18-30'
        when datediff(YEAR, "date_of_birth", current_date) <= 45 then '31-45'
        when datediff(YEAR, "date_of_birth", current_date) <= 64 then '46-64'
        else '65+'
    end as age_group
From {{ source('hospital_raw', 'patients') }}