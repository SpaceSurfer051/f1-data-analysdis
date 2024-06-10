from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

def list_files_in_gcs(bucket_name):
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    files = hook.list(bucket_name=bucket_name)
    print(f"Files in GCS bucket '{bucket_name}': {files}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(dag_id='gcs_connection_test',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    list_files_task = PythonOperator(
        task_id='list_files_in_gcs',
        python_callable=list_files_in_gcs,
        op_kwargs={'bucket_name': 'f1-de-rawdata-bucket'}
    )
    
    list_files_task
