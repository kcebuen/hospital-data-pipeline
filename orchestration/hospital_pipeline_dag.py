from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'DEV404',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hospital_data_pipeline',
    default_args=default_args,
    description='End-to-end hospital data pipeline',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Ingest from Kaggle to S3
    ingest = BashOperator(
        task_id='ingest_kaggle_to_s3',
        bash_command='python /home/nimda/hospital-data-pipeline/ingestion/ingest_kaggle_to_s3.py',
    )

    # Task 2: Load S3 to Snowflake (COPY INTO for all tables)
    load = SQLExecuteQueryOperator(
        task_id='load_s3_to_snowflake',
        conn_id='snowflake_default',
        sql='load_s3_to_snowflake.sql',
    )

    # Task 3: dbt run
    dbt_env = {
	'SNOWFLAKE_PASSWORD': '{{ var.value.snowflake_password }}',
    }

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /home/nimda/hospital-data-pipeline/transformation/hospital_dbt && dbt run',
	env=dbt_env,
	append_env=True,
    )

    # Task 4: dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /home/nimda/hospital-data-pipeline/transformation/hospital_dbt && dbt test',
    	env=dbt_env,
	append_env=True,
	)

    ingest >> load >> dbt_run >> dbt_test
