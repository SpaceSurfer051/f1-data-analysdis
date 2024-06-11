from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

with DAG(
    dag_id="f1_driver_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    def get_driver_data(bucket_name, **kwargs):
        try:
            conf = kwargs["dag_run"].conf

            session_key = conf.get("session_key")

            # 데이터 추출
            url = "https://api.openf1.org/v1/drivers"
            res = requests.get(url)
            res.raise_for_status()  # 요청 실패시 예외
            driver_data = res.json()
            print(session_key, driver_data)

            driver_df = pd.DataFrame(driver_data)

            # NULL 값 처리 (FULL REFRESH)
            driver_df['team_name'] = driver_df.groupby('full_name')['team_name'].transform(lambda x: x.ffill().bfill())
            driver_df['team_colour'] = driver_df.groupby('team_name')['team_colour'].transform(lambda x: x.ffill().bfill())
            driver_df['first_name'] = driver_df.groupby('full_name')['first_name'].transform(lambda x: x.ffill().bfill())
            driver_df['last_name'] = driver_df.groupby('full_name')['last_name'].transform(lambda x: x.ffill().bfill())
            driver_df['headshot_url'] = driver_df.groupby('full_name')['headshot_url'].transform(lambda x: x.ffill())
            driver_df['country_code'] = driver_df.groupby('full_name')['country_code'].transform(lambda x: x.ffill().bfill())

            new_driver_csv = driver_df.to_csv(index=False)

            # GCS 업로드
            file_path = "driver.csv" # session_key에 따라 다른 파일로 추가?

            gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=file_path,
                data=new_driver_csv,
                mime_type="text/csv",
            )

            print("gcs upload complete!!!!")
        
        except Exception as e:
            print(f"Error occurred: {e}")
            raise

    driver_task = PythonOperator(
        task_id="get_driver",
        python_callable=get_driver_data,
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}"},
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id = "load_to_bigquery",
        bucket = "{{ var.value.gcs_bucket_name }}", # 버킷 이름
        source_objects = ["driver.csv"], # 경로
        destination_project_dataset_table = "your_project.your_dataset.your_table",
        source_format = "CSV",
        write_disposition = "WRITE_TRUNCATE", # 데이터를 덮어쓰도록 설정 (또는 WRITE_APPEND)
        gcp_conn_id = "google_cloud_default", # 연결 ID 지정
    )
