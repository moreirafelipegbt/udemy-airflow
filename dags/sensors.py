from airflow import DAG
from airflow import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime
import requests

with DAG('httpsensor', description='httpsensor', schedule_interval=None, start_date=datetime(2023,5,28), catchup=False) as dag:

    def query_api():
        response = requests.get('https://api.publicapis.org/entries')
        
        print(response.text)
        print(response.status_code)

    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='my_connection',
        endpoint='entries',
        poke_interval=5,
        timeout=20
    )

    process_data = PythonOperator(task_id="process_data", python_callable=query_api)

check_api >> process_data