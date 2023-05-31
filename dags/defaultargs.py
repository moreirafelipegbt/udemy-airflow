from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG('exemplo_xcom', description='xcom', schedule_interval=None, start_date=datetime(2023,5,27), catchup=False) as dag:

    def task_write(**kwarg):
        kwarg['ti'].xcom_push(key='valorxcom1', value=10200)

    
    def task_read(**kwarg):
        valor = kwarg['ti'].xcom_pull(key='valorxcom1')
        print(f"Valor recuperado: {valor}")
    
    task1 = PythonOperator(task_id='tsk1', python_callable=task_write)
    task2 = PythonOperator(task_id='tsk2', python_callable=task_read)

    task1 >> task2