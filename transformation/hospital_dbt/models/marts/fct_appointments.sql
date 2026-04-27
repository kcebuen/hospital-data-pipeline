Select
    appointment_id,
    appointment_date,
    appointment_time,
    reason_for_visit,
    status,
    patient_id,
    patient_name,
    age,
    age_group,
    doctor_id,
    doctor_name,
    specialization,
    hospital_branch,
    case
        when status = 'No-Show' then 1 else 0
    end as no_show,
    case
        when status = 'Cancelled' then 1 else 0
    end as is_cancelled
From {{ ref('int_patient_appointments') }}