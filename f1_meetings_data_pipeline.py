from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pandas as pd
import io


def fetch_and_upload_meetings_data(bucket_name, execution_date, **kwargs):

    url = "https://api.openf1.org/v1/meetings"

    response = requests.get(url)

    if response.status_code == 200:
        response.encoding = "utf-8"
        meeting_data = response.json()
    else:
        print(f"Failed to fetch data for meetings. Status code: {response.status_code}")

    df = pd.DataFrame(meeting_data)

    # DataFrame을 CSV 형식으로 변환
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_data = csv_buffer.getvalue()

    # GCS에 업로드
    gcs_path = f"meetings/meeting_data_" + execution_date + ".csv"

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=gcs_path,
        data=csv_data,
        mime_type="text/csv",
    )


with DAG(
    dag_id="f1_meetings_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    fetch_and_upload_meetings_data_task = PythonOperator(
        task_id="fetch_and_upload_meetings_data",
        python_callable=fetch_and_upload_meetings_data,
        op_kwargs={
            "bucket_name": "{{ var.value.gcs_bucket_name }}",
            "execution_date": "{{ ds }}",
        },
        provide_context=True,
    )

fetch_and_upload_meetings_data_task
