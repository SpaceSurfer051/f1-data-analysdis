from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 15),
}

DATASET = "basic" 
TABLE = "forestfires"
gcp_id = 'google_cloud_default'

with DAG('example_bigquery_dag',
         default_args=default_args,
         schedule_interval=None) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET,
        gcp_conn_id=gcp_id
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE,
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "month", "type": "STRING", "mode": "NULLABLE"},
            {"name": "day", "type": "STRING", "mode": "NULLABLE"},
        ],
        gcp_conn_id=gcp_id
    )



    create_dataset >> create_table