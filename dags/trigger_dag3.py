from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('dag_complexa', description="DAG complexa", schedule_interval=None, start_date=datetime(2023,5,26), catchup=False) as dag:

    task1 = BashOperator(task_id="tsk1",bash_command="exit 1")
    task2 = BashOperator(task_id="tsk2",bash_command="exit 1")
    task3 = BashOperator(task_id="tsk3",bash_command="exit 1")
    task4 = BashOperator(task_id="tsk4",bash_command="exit 1")
    task5 = BashOperator(task_id="tsk5",bash_command="exit 1")
    task6 = BashOperator(task_id="tsk6",bash_command="exit 1")
    task7 = BashOperator(task_id="tsk7",bash_command="exit 1")
    task8 = BashOperator(task_id="tsk8",bash_command="exit 1")
    task9 = BashOperator(task_id="tsk9",bash_command="sleep 5", trigger_rule='one_failed')

task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> [task7, task8, task9]