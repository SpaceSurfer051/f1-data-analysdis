from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import requests
import time
from datetime import datetime
import pandas as pd


def save_session_data_to_gcs(bucket_name, **kwargs):
    total_sessions = requests.get(f"https://api.openf1.org/v1/sessions")
    while total_sessions.status_code != 200:
        time.sleep(5)
        total_sessions = requests.get(f"https://api.openf1.org/v1/sessions")
    total_sessions = total_sessions.json()

    total_sessions_df = pd.DataFrame(total_sessions)
    total_sessions_csv = total_sessions_df.to_csv(index=False)

    file_path = f"sessions/sessions.csv"

    gcs_hook = GCSHook(gcp_conn_id="gcs_connection")
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=file_path,
        data=total_sessions_csv,
        mime_type="text/csv",
    )

    print("Session data upload to GCS was complete!!")


def load_to_bq(files, bucket_name, bigquery_project_dataset, **kwargs):
    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=bucket_name,
        source_objects=files,
        destination_project_dataset_table=bigquery_project_dataset+".session",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="gcp_conn",
    )

    return gcs_to_bq.execute(kwargs)


with DAG(
    dag_id="f1_session_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    save_session_to_gcs = PythonOperator(
        task_id="save_session_to_gcs",
        python_callable=save_session_data_to_gcs,
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}"},
    )

    load_to_bq_task = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_to_bq,
        provide_context=True,
        op_args=["sessions/sessions.csv"],
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}",
                   "bigquery_project_dataset": "{{ var.value.bigquery_project_dataset }}"},
    )

    save_session_to_gcs >> load_to_bq_task
