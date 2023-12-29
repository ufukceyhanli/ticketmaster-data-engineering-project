from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import json
import urllib.request
from airflow.models import Variable

# Constants
API_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
API_KEY = Variable.get("TICKETMASTER_API_KEY")
LOCAL_DATA_PATH = "/opt/airflow/data/historical_data.csv.gz"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
}

dag = DAG(
    'new_dag',
    description='New DAG',
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)

def extract_past_data(COUNTRY_CODE, START_DATE, END_DATE, **kwargs):
    url = f"{API_URL}?countryCode={COUNTRY_CODE}&apikey={API_KEY}&size=200&startDateTime={START_DATE}&endDateTime={END_DATE}"
    req = urllib.request.Request(url)
    req.get_method = lambda: 'GET'
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.getcode() == 200:
                data = json.loads(response.read())
                events_data = data.get("_embedded", {}).get("events", [])
                past_df = pd.DataFrame(events_data)
                past_df.to_csv(LOCAL_DATA_PATH, index=True, compression="gzip")
                return True
    except Exception as e:
        # Add appropriate error handling (logging, notifications, etc.)
        raise

extract_past_data_to_local_task = PythonOperator(
    task_id='extract_past_data_to_local_task',
    python_callable=extract_past_data,
    provide_context=True,
    dag=dag,
    op_kwargs={
        'COUNTRY_CODE': "NL",
        'START_DATE': "2024-02-01T14:00:00Z",
        'END_DATE' : "2024-02-05T14:00:00Z"
    }
)

# Define task dependencies explicitly
extract_past_data_to_local_task  # No downstream tasks for now
