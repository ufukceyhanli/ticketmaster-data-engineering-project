from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import os
import json
import urllib.request


GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BQ_DATASET_PROD = os.environ.get('BIGQUERY_DATASET', 'earthquake_prod')
API_KEY = os.environ.get('TICKETMASTER_API_KEY')

SERVICE_ACCOUNT_JSON_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
external_table_name = 'daily_staging'

EXECUTION_MONTH = '{{ logical_date.strftime("%-m") }}'
EXECUTION_DAY = '{{ logical_date.strftime("%-d") }}'
EXECUTION_HOUR = '{{ logical_date.strftime("%-H") }}'
EXECUTION_DATETIME_STR = '{{ logical_date.strftime("%m%d%H") }}'

def extract_past_year(country_code, start_date, end_date, **kwargs):
    try:
        dataset_url = f"https://app.ticketmaster.com/discovery/v2/events?apikey={API_KEY}&locale=*&startDateTime={start_date}T00:00:00Z&endDateTime={end_date}T00:00:00Z&size=200&countryCode={country_code}"
        print(dataset_url)
        req = urllib.request.Request(dataset_url)
        with urllib.request.urlopen(req) as response:
            if response.getcode() == 200:
                data = json.loads(response.read())
                events_data = data.get("_embedded", {}).get("events", [])
                past_year_df = pd.DataFrame(events_data)
                
                file_name = "historical_data"
                local_file_path = f"/opt/airflow/data/{file_name}.csv.gz"
                past_year_df.to_csv(local_file_path, index=False, compression="gzip")
                
                kwargs['ti'].xcom_push(key="general", value={"local_file_path": local_file_path, "file_name": file_name})
                return True
    except Exception as e:
        print(str(e))

def upload_to_bucket(**kwargs):
    print(kwargs)
    print(kwargs['ti'])
    dict_data = kwargs['ti'].xcom_pull(key="general",task_ids="extract_data_to_local_task")


    local_file_path = dict_data['local_file_path']
    blob_name = f"events/{dict_data['file_name']}.csv.gz"

    """ Upload data to a bucket"""
    storage_client = storage.Client.from_service_account_json(
        SERVICE_ACCOUNT_JSON_PATH)

    bucket = storage_client.get_bucket(GCP_GCS_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file_path)

    dict_data["bucket_file_path"] = f"gs://{GCP_GCS_BUCKET}/{blob_name}"
    kwargs['ti'].xcom_push(key="general2", value=dict_data)
    


dag = DAG(
    'historical_DAG',
    description='Historical DAG', 
    schedule_interval='@once',
    start_date=datetime(2023, 4, 22), 
    catchup=False, 
    max_active_runs=1
)

extract_data_to_local_task = PythonOperator(
    task_id=f"extract_data_to_local_task",
    python_callable=extract_past_year,
    provide_context=True,
    dag=dag,
    op_kwargs={
        'country_code': "NL",
        'start_date': "2024-01-01",
        'end_date' : "2024-06-30"
    }
)

local_to_gcs_task = PythonOperator(
    task_id=f"local_to_gcs_task",
    python_callable=upload_to_bucket,
    provide_context=True,
    dag=dag
)

spark_transformation_task = BashOperator(
    task_id=f"spark_transformation_task",
    bash_command="python /opt/airflow/dags/spark_job.py --input_file '{{ ti.xcom_pull(key='general2',task_ids='local_to_gcs_task')['bucket_file_path']}}' ",
    dag=dag
)

clear_local_files_task = BashOperator(
    task_id=f"clear_local_files_task",
    bash_command="rm {{ ti.xcom_pull(key='general2',task_ids='local_to_gcs_task')['local_file_path'] }}",
    dag=dag
)


extract_data_to_local_task #>> local_to_gcs_task >> spark_transformation_task >> clear_local_files_task
