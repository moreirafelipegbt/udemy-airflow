from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import random

with DAG('branchtest', description="Branch dag", schedule_interval=None, start_date=datetime(2023,5,28), catchup=False) as dag:

    def gera_num_aleatorio() -> int:
        return random.randint(1,100)

    
    def avalia_num_aleatorio(**context):
        numero = context['task_instance'].xcom_pull(task_ids="gera_num_aleatorio_task")

        if numero % 2 == 0:
            return 'par_task'
        else:
            return 'impar_task'

    gera_num_aleatorio_task = PythonOperator(
        task_id="gera_num_aleatorio_task",
        python_callable=gera_num_aleatorio   
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=avalia_num_aleatorio,
        provide_context=True
    )

    par_task = BashOperator(task_id="par_task", bash_command="echo Número par")
    impar_task = BashOperator(task_id="impar_task", bash_command="echo Número impar")

    gera_num_aleatorio_task >> branch_task
    branch_task >> par_task
    branch_task >> impar_task