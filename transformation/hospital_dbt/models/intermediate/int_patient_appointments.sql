Select
    a.appointment_id,
    a.appointment_date,
    a.appointment_time,
    a.reason_for_visit,
    a.status,
    p.patient_id,
    p.full_name as patient_name,
    p.age,
    p.age_group,
    d.doctor_id,
    d.full_name as doctor_name,
    d.specialization,
    d.hospital_branch
From {{ ref('stg_appointments') }} a
Join {{ ref('stg_patients') }} p
    on a.patient_id = p.patient_id
Join {{ ref('stg_doctors') }} d
    on a.doctor_id = d.doctor_id