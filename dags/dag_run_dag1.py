from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator

with DAG('dag_run_dag1', description="DAG running DAG 1", schedule_interval=None, start_date=datetime(2023,5,27), catchup=False) as dag:

    task1 = BashOperator(task_id="tsk1",bash_command="sleep 5")
    task2 = TriggerDagRunOperator(task_id="tsk2", trigger_dag_id="dag_run_dag2")

task1 >> task2