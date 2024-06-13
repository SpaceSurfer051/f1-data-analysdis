from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import time
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable



@dag(
    dag_id="f1_meeting_stint_pipeline",
    start_date=datetime(2023, 2, 1),
    schedule_interval="0 0 1 * *",
    catchup=True,
    tags=['f1', 'data_pipeline']
)
def f1_meeting_session_dag(**context):
    bucket_name = Variable.get("gcs_bucket_name")
    @task
    def get_meeting_key_from_open_f1(**context):
        logical_date =   context['ds'] # logical date는 'ds'에 저장됨     
        month_end = logical_date
        print(f'{month_end}')
        month_start = logical_date[:7]+'-01'
        print(f'{month_start}')
                
        meetings_response = requests.get(f"https://api.openf1.org/v1/meetings?date_start>={month_start}&date_start<={month_end}")   
        while meetings_response.status_code != 200:
            time.sleep(5)
            meetings_response = requests.get(f"https://api.openf1.org/v1/meetings?date_start>={month_start}&date_start<={month_end}")   
        
        month_meetings = meetings_response.json()
            
        print(f"success meeting data{month_meetings}")
        
        meeting_list = [l["meeting_key"] for l in month_meetings]
        return meeting_list
        
        
    @task
    def f1_process_stint_data(meeting_list):
            stint_dfs = []
            for key in meeting_list:
                stint_response = requests.get(f"https://api.openf1.org/v1/stints?meeting_key={key}")
                if stint_response.status_code == 200:
                    stint = stint_response.json()
                    stint_df = pd.DataFrame(stint)
                    stint_dfs.append(stint_df)
                else:
                    print(f"Failed to fetch stint data for meeting key {key}: {stint_response.status_code}")

            if stint_dfs:
                combined_stint_df = pd.concat(stint_dfs, ignore_index=True)
                print("combine is coplete")
            else:
                print("No stint data available.")
            
            stint_out_csv = combined_stint_df.to_csv(index=False)
            
            return stint_out_csv
    
    
    @task
    def save_session_data_to_gcs(stint_out_csv,**context):
        bucket_name = Variable.get("gcs_bucket_name")
        acess_month  = context['ds'][:7]
        file_path = f"stint/{acess_month}_stint.csv"

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=file_path,
            data=stint_out_csv,
            mime_type="text/csv",
        )

        print("Session data upload to GCS was complete!!")
        
        return file_path
    
    
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="{{ var.value.gcs_bucket_name }}",
        source_objects=["{{ ti.xcom_pull(task_ids='save_session_data_to_gcs') }}"],
        destination_project_dataset_table="{{ var.value.bigquery_project_dataset }}.stint",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        gcp_conn_id="google_cloud_default",
    )

        
    meeting_list = get_meeting_key_from_open_f1()
    stint_out_csv = f1_process_stint_data(meeting_list)
    file_path = save_session_data_to_gcs(stint_out_csv)
    file_path >> gcs_to_bq_task
    
    # GCS에 데이터 업로드
    
    # BigQuery로 데이터 로드

# DAG 인스턴스 생성

dag_instance = f1_meeting_session_dag()
