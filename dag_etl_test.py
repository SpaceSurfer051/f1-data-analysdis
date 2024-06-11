from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'gcs_to_bq_autodetect',
    default_args=default_args,
    description='Load data from GCS to BigQuery with schema autodetection',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

gcs_to_bq = GCSToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='f1-de-rawdata-bucket',
    source_objects=['trainer.csv'],
    destination_project_dataset_table='f1-data-3rd.basic.new_trainer',
    source_format='CSV',
    write_disposition='WRITE_APPEND',
    autodetect=True,
    google_cloud_storage_conn_id='google_cloud_default',
    bigquery_conn_id='google_cloud_default',
    dag=dag,
)
