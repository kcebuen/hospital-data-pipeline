Select
    "appointment_id" as appointment_id,
    "patient_id" as patient_id,
    "doctor_id" as doctor_id,
    "appointment_date" as appointment_date,
    "appointment_time" as appointment_time,
    cast("appointment_date" as timestamp) || "appointment_time"::time as appointment_datetime,
    "reason_for_visit" as reason_for_visit,
    "status" as status
From {{ source('hospital_raw', 'appointments') }}