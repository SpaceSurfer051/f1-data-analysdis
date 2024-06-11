from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import requests
import pandas as pd
import time


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


def list_gcs_files(bucket_name, prefix, **kwargs):
    hook = GCSHook(gcp_conn_id="gcp_conn")
    files = hook.list(bucket_name, prefix=prefix)
    print("laps files: ", files)
    return files


def load_to_bq(files, bucket_name, bigquery_project_dataset, **kwargs):
    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=bucket_name,
        source_objects=files,
        destination_project_dataset_table=bigquery_project_dataset+".lap",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="gcp_conn",
    )

    return gcs_to_bq.execute(kwargs)


with DAG(
    dag_id="f1_laps_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    laps_task = PythonOperator(
        task_id="get_laps",
        python_callable=get_laps,
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}"},
    )

    get_gcs_files = PythonOperator(
        task_id="get_gcs_files",
        python_callable=list_gcs_files,
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}", "prefix": "laps/"},
    )

    load_to_bq_task = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_to_bq,
        provide_context=True,
        op_args=[get_gcs_files.output],
        op_kwargs={"bucket_name": "{{ var.value.gcs_bucket_name }}",
                   "bigquery_project_dataset": "{{ var.value.bigquery_project_dataset }}"},
    )

    laps_task >> get_gcs_files >> load_to_bq_task
