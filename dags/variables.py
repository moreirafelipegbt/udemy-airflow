from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

with DAG('variaveis', description="variaveis", schedule_interval=None, start_date=datetime(2023,5,28), catchup=False) as dag:

    def print_variable(**context):
        minha_var = Variable.get('myvar')
        print(f'O valor da minha variável é {minha_var}')
    
    task1 = PythonOperator(task_id="tsk1", python_callable=print_variable)

    task1