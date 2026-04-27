Select
    "treatment_id" as treatment_id,
    "appointment_id" as appointment_id,
    "treatment_type" as treatment_type,
    "description" as description,
    cast("cost" as number(10,2)) as cost,
    "treatment_date" as treatment_date
From {{ source('hospital_raw', 'treatments') }}