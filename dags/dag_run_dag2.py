from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('dag_run_dag2', description="DAG running DAG 2", schedule_interval=None, start_date=datetime(2023,5,27), catchup=False) as dag:

    task1 = BashOperator(task_id="tsk1",bash_command="exit 1")
    task2 = BashOperator(task_id="tsk2",bash_command="exit 1")

task1 >> task2