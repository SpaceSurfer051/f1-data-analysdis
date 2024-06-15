from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import pandas as pd
import time
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from dateutil.relativedelta import relativedelta



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,  # 재시도 횟수
    'retry_delay': timedelta(minutes=1),  # 재시도 간격
}



@dag(
    dag_id="f1_meeting_stint_pipeline",
    default_args = default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 0 1 * *",
    max_active_runs=5,
    catchup=True,
    tags=['f1', 'data_pipeline']
)
def f1_meeting_session_dag(**context):
    bucket_name = Variable.get("gcs_bucket_name")
    @task
    def get_meeting_key_from_open_f1(**context):
        
        
        previous_month_start = context['logical_date'] + timedelta(days=1)
        previous_month_end =previous_month_start+ relativedelta(months=1)-timedelta(days=1)
        
        month_start = previous_month_start.strftime("%Y-%m-%d")
        month_end = previous_month_end.strftime("%Y-%m-%d")
        print(f'start month{month_start}')
        print(f'end month{month_end}')
        
        meetings_response = requests.get(f"https://api.openf1.org/v1/meetings?date_start>={month_start}&date_start<={month_end}")   
        while meetings_response.status_code != 200:
            time.sleep(5)
            meetings_response = requests.get(f"https://api.openf1.org/v1/meetings?date_start>={month_start}&date_start<={month_end}")   
        
        month_meetings = meetings_response.json()
            
        print(f"success meeting data{month_meetings}")
        
        meeting_list = [l["meeting_key"] for l in month_meetings]
        if meeting_list is None:
            print("No file path provided for BigQuery loading. Skipping task.")
            raise AirflowSkipException("No file path provided for BigQuery loading")
        return meeting_list
        
        
    @task
    def f1_process_stint_data(meeting_list):
            stint_dfs = []
            s_key =[]
            for  i in meeting_list:
                session_json = requests.get(
                        f"https://api.openf1.org/v1/sessions?meeting_key={i}"
                        ).json()
                
                for session_list in session_json:
                    if session_list["session_type"] in ["Qualifying", "Race"]:
                        s_key.append(session_list["session_key"])
                        
            for key in s_key:
                stint_response = requests.get(f"https://api.openf1.org/v1/stints?session_key={key}")
                stint = stint_response.json()
                stint_df = pd.DataFrame(stint)
                stint_dfs.append(stint_df)
                
            if stint_dfs:
                combined_stint_df = pd.concat(stint_dfs, ignore_index=True)
                print("combine is coplete")
            else:
                print("No stint data available.")
                '''
            combined_stint_df['tyre_age_at_start'] = combined_stint_df['tyre_age_at_start'].fillna(0)
            combined_stint_df['tyre_age_at_start'] = combined_stint_df['tyre_age_at_start'].astype(int)
            combined_stint_df['driver_number']  = combined_stint_df['driver_number'].dropna(axis=0)
            combined_stint_df['driver_number'] = combined_stint_df['driver_number'].astype(int)
            '''
            stint_out_csv = combined_stint_df.to_csv(index=False)
            
            return combined_stint_df
    
    
    @task
    def save_session_data_to_gcs(stint_out_csv,**context):
        bucket_name = Variable.get("gcs_bucket_name")
        acess_month  = (context['logical_date'] + timedelta(days=1)).strftime("%Y-%m-%d")
        file_path = f"stint/{acess_month}_stint.csv"
        stint_data= stint_out_csv.to_csv(index=False)

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=file_path,
            data=stint_data,
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
    
    @task.branch()
    def branch_task(meeting_list):
        if not meeting_list:
            return 'skip_task'
        return 'f1_process_stint_data'

    skip_task = EmptyOperator(task_id='skip_task')
    
    meeting_list = get_meeting_key_from_open_f1()
    branch_result = branch_task(meeting_list)
    
    process_task = f1_process_stint_data(meeting_list)
    save_task = save_session_data_to_gcs(process_task)
    
    branch_result >> process_task >> save_task >> gcs_to_bq_task
    
    branch_result >> skip_task
    


dag_instance = f1_meeting_session_dag()
