from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
import requests
import pandas as pd
import time


with DAG(
    dag_id="f1_laps_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    def get_laps(bucket_name, **kwargs):
        conf = kwargs["dag_run"].conf

        session_key = conf.get("session_key")

        new_lap_datas = requests.get(
            f"https://api.openf1.org/v1/laps?session_key={session_key}"
        )
        while new_lap_datas.status_code != 200:
            time.sleep(5)
            new_lap_datas = requests.get(
                f"https://api.openf1.org/v1/laps?session_key={session_key}"
            )
        new_lap_datas = new_lap_datas.json()
        print(session_key, new_lap_datas)

        new_lap_df = pd.DataFrame(new_lap_datas)

        new_lap_csv = new_lap_df.to_csv(index=False)

        file_path = f"laps/laps_{session_key}.csv"

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=file_path,
            data=new_lap_csv,
            mime_type="text/csv",
        )

        print("gcs upload complete!!!!")
        

    laps_task = PythonOperator(
        task_id="get_laps",
        python_callable=get_laps,
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}"},
    )
