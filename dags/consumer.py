from airflow import DAG
from airflow import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

with DAG('consumer', description='producer', schedule=[mydataset], start_date=datetime(2023,5,28), catchup=False) as dag:

    def my_file():
        dataset = pd.read_csv("/opt/airflow/data/Churn_new.csv", sep=";")
        dataset.to_csv("/opt/airflow/data/Churn_new2.csv", sep=";")
    
    t1 = PythonOperator(task_id="t1", python_callable=my_file, provide_context=True)

t1