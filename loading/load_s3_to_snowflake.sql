COPY INTO HOSPITAL_DB.RAW.treatments
FROM @hospital_db.raw.hospital_ds/kaggle-ds/hospital-management-dataset/20260426/treatments.csv
FILE_FORMAT = 'hospital_db.raw.csv_ff_load';

COPY INTO HOSPITAL_DB.RAW.appointments
FROM @hospital_db.raw.hospital_ds/kaggle-ds/hospital-management-dataset/20260426/appointments.csv
FILE_FORMAT = 'hospital_db.raw.csv_ff_load';

COPY INTO HOSPITAL_DB.RAW.patients
FROM @hospital_db.raw.hospital_ds/kaggle-ds/hospital-management-dataset/20260426/patients.csv
FILE_FORMAT = 'hospital_db.raw.csv_ff_load';

COPY INTO HOSPITAL_DB.RAW.billing
FROM @hospital_db.raw.hospital_ds/kaggle-ds/hospital-management-dataset/20260426/billing.csv
FILE_FORMAT = 'hospital_db.raw.csv_ff_load';

COPY INTO HOSPITAL_DB.RAW.doctors
FROM @hospital_db.raw.hospital_ds/kaggle-ds/hospital-management-dataset/20260426/doctors.csv
FILE_FORMAT = 'hospital_db.raw.csv_ff_load';