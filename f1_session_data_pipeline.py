from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
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

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=file_path,
        data=total_sessions_csv,
        mime_type="text/csv",
    )

    print("Session data upload to GCS was complete!!")


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
