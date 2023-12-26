import urllib.request
import pandas as pd
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import os

def extract_past_data(period, stocks, **kwargs):
    url = "https://app.ticketmaster.com/discovery/v2/events.json?countryCode=NL&apikey=z8N0z01P5zAe4W7APxFl5wUvn5UzvJ8G&size=200&startDateTime=2024-02-01T14:00:00Z&endDateTime=2024-02-05T14:00:00Z"
    req = urllib.request.Request(url)
    req.get_method = lambda: 'GET'
    response = urllib.request.urlopen(req)
    
    if response.getcode() == 200:
        data = json.loads(response.read())

        # Assuming the relevant data is in the 'payload' key, adjust this according to the actual structure of the API response
        events_data = data["_embedded"]["events"]

        # Convert the data into a DataFrame
        past_df = pd.DataFrame(events_data)
    
    file_name = "historical_data"
    local_file_path = f"/opt/airflow/data/{file_name}.csv.gz"
    
    past_df.to_csv(local_file_path, index=True, compression="gzip")

dag = DAG('new_dag', description='new dag', schedule_interval='@once',
          start_date=datetime(2023, 4, 22), catchup=False, max_active_runs=1)

extract_past_data_to_local_task = PythonOperator(
    task_id=f"extract_past_data_to_local_task",
    python_callable=extract_past_data,
    provide_context=True,
    dag=dag,
    op_kwargs={
        "period": 60,
        "stocks": ['AAPL', 'MSFT']
    }
)

extract_past_data_to_local_task
