Select
    treatment_id,
    treatment_type,
    description,
    cost,
    treatment_date,
    bill_id,
    bill_date,
    amount,
    payment_method,
    payment_status,
    appointment_id,
    patient_id,
    case when payment_status = 'Paid' then amount else 0 end as paid_amount,
    case when payment_status = 'Pending' then amount else 0 end as pending_amount,
    case when payment_status = 'Failed' then amount else 0 end as failed_amount
From {{ ref('int_treatment_billing') }}