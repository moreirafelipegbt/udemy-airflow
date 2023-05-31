from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator

with DAG('dummy', description="dummy",
        schedule_interval=None, start_date=datetime(2023,5,28), catchup=False) as dag:
    
    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5")
    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5")
    task3 = BashOperator(task_id="tsk3", bash_command="sleep 5")
    task4 = BashOperator(task_id="tsk4", bash_command="sleep 5")
    task5 = BashOperator(task_id="tsk5", bash_command="sleep 5")
    taskdummy = DummyOperator(task_id="taskdummy")

    [task1, task2, task3] >> taskdummy >> [task4, task5]