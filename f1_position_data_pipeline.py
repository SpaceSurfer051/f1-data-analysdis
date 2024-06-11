from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.google.cloud.operators.gcs import GCSHook
import requests
import logging


# task 정의
def get_latest_records(**kwargs):
    conf = kwargs["dag_run"].conf
    session_key = conf.get("session_key")

    bucket_name = Variable.get("gcs_bucket_name")

    try:
        # 세션(경기)에 맞는 데이터 가져오기
        response = requests.get(
            "https://api.openf1.org/v1/position?session_key={}".format(session_key)
        ).json()

        position_df = pd.DataFrame(response)

        latest_records = (
            position_df.sort_values(by="date", ascending=False)
            .groupby("driver_number")
            .first()
        )

        # GCS에 저장(csv 파일))
        file_name = "position_{}.csv".format(session_key)
        position_csv = latest_records.to_csv(file_name, index=False)

        gcs_hook = GCSHook(gcp_conn_id="gcs_connection")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name = file_name,
            filename=file_name,
            data=position_csv,
            mime_type="text/csv",
        )
    except Exception as e:
        logging.error("An error occurred: %s", e)
        raise

    logging.info("Load Done!")


# DAG 정의
with DAG(
    dag_id="f1_position_data_pipeline",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:

    # Operator
    postion_task = PythonOperator(
        task_id="get_latest_records",
        python_callable=get_latest_records,
        provide_context = True,
    )
