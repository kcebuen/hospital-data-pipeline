Select
    t.treatment_id,
    t.treatment_type,
    t.description,
    t.cost,
    t.treatment_date,
    b.bill_id,
    b.bill_date,
    b.amount,
    b.payment_method,
    b.payment_status,
    a.appointment_id,
    b.patient_id
From {{ ref('stg_treatments') }} t
Join {{ ref('stg_billing') }} b
    on t.treatment_id = b.treatment_id
Join {{ ref('stg_appointments') }} a
    on t.appointment_id = a.appointment_id