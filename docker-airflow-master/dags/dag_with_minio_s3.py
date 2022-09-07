from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'shadrach',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id = 'dag_with_s3_v01',
    start_date = datetime(2022,6,9)
    schedule_interval='@daily',
    default_args = default_args
) as dag:
    task1 = s3keysensor(
        task_id= 'sensor_minio_s3',
        bucket_name = 'airflow',
        bucket_key = 'data.csv',
        aws_conn_id = 'minio_conn'
    )