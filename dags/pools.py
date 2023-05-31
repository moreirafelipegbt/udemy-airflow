from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('pool', description="pool dags", schedule_interval=None, start_date=datetime(2023,5,28), catchup=False)  as dag:

    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", pool="mypool")
    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", pool="mypool", priority_weight=5)
    task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", pool="mypool")
    task4 = BashOperator(task_id="tsk4", bash_command="sleep 5", pool="mypool", priority_weight=10)
