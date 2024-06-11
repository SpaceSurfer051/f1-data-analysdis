from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
import requests
import logging


# task 정의
def get_records(bucket_name, **kwargs):
    conf = kwargs["dag_run"].conf
    session_key = conf.get("session_key")

    try:
        # 세션(경기)에 맞는 데이터 가져오기
        response = requests.get(
            "https://api.openf1.org/v1/position?session_key={}".format(session_key)
        ).json()

        position_df = pd.DataFrame(response)

        # GCS에 저장(csv 파일))
        file_name = "position_full_{}.csv".format(session_key)
        file_path = "position_full/{}".format(file_name)
        position_csv = position_df.to_csv(file_name, index=False)

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=file_path,
            filename=file_name,
            data=position_csv,
            mime_type="text/csv",
        )
    except Exception as e:
        logging.error("An error occurred: %s", e)
        raise

    logging.info("Load Done!")


# gcs position/파일 리스트
def list_gcs_files(bucket_name, prefix):
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    files = hook.list(bucket_name, prefix=prefix)
    return files


# GCS -> Bigquery Load
def load_gcs_to_bq(**kwargs):
    files = kwargs["ti"].xcom_pull(task_ids="list_and_load_files_task")
    load_task = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        bucket=bucket_name,
        source_objects=files,
        destination_project_dataset_table=bigquery_project_dataset + ".position_full",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        field_delimiter=",",
    )
    load_task.execute(kwargs)


# DAG 정의
with DAG(
    dag_id="f1_position_full_data_pipeline",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:

    bucket_name = Variable.get("gcs_bucket_name")
    bigquery_project_dataset = Variable.get("bigquery_project_dataset")

    # Operator
    postion_task = PythonOperator(
        task_id="get_records",
        python_callable=get_records,
        op_kwargs={"bucket_name": bucket_name},
        provide_context=True,
    )

    list_and_load_files_task = PythonOperator(
        task_id="list_and_load_files_task",
        python_callable=list_gcs_files,
        op_kwargs={"bucket_name": bucket_name, "prefix": "position_full/"},
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_gcs_to_bq,
        op_kwargs={
            "bucket_name": bucket_name,
            "bigquery_project_dataset": bigquery_project_dataset,
        },
        provide_context=True,
    )

    postion_task >> list_and_load_files_task >> load_task
