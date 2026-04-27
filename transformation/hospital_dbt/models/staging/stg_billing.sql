Select
    "bill_id" as bill_id,
    "patient_id" as patient_id,
    "treatment_id" as treatment_id,
    "bill_date" as bill_date,
    cast("amount" as number(10,2)) as amount,
    "payment_method" as payment_method,
    "payment_status" as payment_status
From {{ source('hospital_raw', 'billing') }}