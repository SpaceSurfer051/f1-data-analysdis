from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pandas as pd
import io


# GCS에서 파일을 다운로드하고 DataFrame으로 로드하는 함수
def load_csv_from_gcs(bucket_name, object_name):
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)
    return pd.read_csv(io.StringIO(file_content.decode("utf-8")))


def fetch_and_upload_pit_data(bucket_name, execution_date, object_name, **kwargs):

    existing_df = load_csv_from_gcs(bucket_name, object_name)

    def fetch_session_keys():
        url = "https://api.openf1.org/v1/sessions?date_start%3E2023-06-01&session_type=Race"
        response = requests.get(url)
        if response.status_code == 200:
            sessions_data = response.json()
            return [session["session_key"] for session in sessions_data]
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return []

    session_keys = fetch_session_keys()
    new_pit_data = []
    url2 = "https://api.openf1.org/v1/pit?session_key={}"

    for session_key in session_keys:
        response2 = requests.get(url2.format(session_key))
        if response2.status_code == 200:
            pit_data = response2.json()
            for pit in pit_data:
                new_pit_data.append(
                    {
                        "driver_number": pit["driver_number"],
                        "lap_number": pit["lap_number"],
                        "pit_duration": pit["pit_duration"],
                        "meeting_key": pit["meeting_key"],
                        "session_key": pit["session_key"],
                    }
                )
        else:
            print(
                f"Failed to fetch data for session_key {session_key}. Status code: {response2.status_code}"
            )

    new_df = pd.DataFrame(new_pit_data)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates()

    # DataFrame을 CSV 형식으로 변환
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # GCS에 업로드
    # date_str = datetime.now().strftime("%Y%m%d")
    gcs_path = f"pit/pit_stop_data_" + execution_date + ".csv"

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=gcs_path,
        data=csv_data,
        mime_type="text/csv",
    )


with DAG(
    dag_id="f1_pit_stop_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    fetch_and_upload_pit_data_task = PythonOperator(
        task_id="fetch_and_upload_pit_data",
        python_callable=fetch_and_upload_pit_data,
        op_kwargs={
            "bucket_name": "{{ var.value.gcs_bucket_name }}",
            "object_name": "{{ var.value.gcs_basic_pit_data}}",
            "execution_date": "{{ ds }}",
        },
        provide_context=True,
    )

fetch_and_upload_pit_data_task
