from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_s3_dag',
    default_args=default_args,
    description='ETL DAG to extract, transform, and load data to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 8, 7),
    catchup=False,
    tags=['etl', 's3', 'data_pipeline']
) as dag:

    run_pyspark_job = BashOperator(
        task_id='run_pyspark_job',
        bash_command='python scripts_spark/etl_prod_s3.py',
    )

    run_pyspark_job