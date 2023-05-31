from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

import json
import os

default_args = {
    'depends_on_past': False,
    'email': ['felipe.moreira@globant.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    'wind_turbine',
    description='Wind turbine data',
    schedule_interval=None,
    start_date=datetime(2023,5,29),
    catchup=False,
    default_args=default_args,
    default_view='graph',
    doc_md='## Dag para registrar dados de turbina eólica.'
) as dag:

    group_check_temp = TaskGroup('group_check_temp')
    group_database = TaskGroup('group_database')

    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath = Variable.get('path_file'),
        fs_conn_id = 'fs_default',
        poke_interval = 10
    )
      
    #SQL statements
    sql_create_table = '''
        CREATE TABLE IF NOT EXISTS SENSORS (
            sidtemp VARCHAR,
            powerfactor VARCHAR,
            hydraulicpressure VARCHAR,
            temperature VARCHAR,
            timestamp VARCHAR
        )
    '''

    sql_insert_data = '''
        INSERT INTO sensors (idtemp, powerfactor, hydraulicpressure, temperature, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    '''

    def process_file(**kwarg):
        with open(Variable.get('path_file')) as f:
            data = json.load(f)
            kwarg['ti'].xcom_push(key='idtemp', value=data['idtemp'])
            kwarg['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
            kwarg['ti'].xcom_push(key='hydraulicpressure', value=data['hydraulicpressure'])
            kwarg['ti'].xcom_push(key='temperature', value=data['temperature'])
            kwarg['ti'].xcom_push(key='timestamp', value=data['timestamp'])
        os.remove(Variable.get('path_file'))

    get_data = PythonOperator(
                task_id='get_data',
                python_callable=process_file,
                provide_context = True
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='pg_connection',
        sql=sql_create_table,
        task_group=group_database
    )

    insert_data = PostgresOperator(
                                    task_id='insert_data',
                                    postgres_conn_id='postgres',
                                    parameters=('{{ t1.xcom_pull(task_ids="get_data", key="idtemp") }}',
                                                '{{ ti.xcom_pull(task_ids="get_data", key="powerfactor") }}',
                                                '{{ ti.xcom_pull(task_ids="get_data", key="hydraulicpressure") }}',
                                                '{{ ti.xcom_pull(task_ids="get_data", key="temperature") }}',
                                                '{{ ti.xcom_pull(task_ids="get_data", key="timestamp") }}'
                                    ),
                                    sql = sql_insert_data,
                                    task_group=group_database
                                )

    send_email_normal = EmailOperator(
        task_id='send_email_normal',
        to='felipe.moreira@globant.com',
        subject='Airflow advise',
        html_content='''
            <h3>Report de temperatura</h3>
            <p>Dag: windturbine</p>
        ''',
        task_group = group_check_temp
    )

    send_email_allert = EmailOperator(
        task_id='send_email_allert',
        to='felipe.moreira@globant.com',
        subject='Airflow advice',
        html_content='''
            <h3>Alerta de temperatura</h3>
            <p>Dag: windturbine</p>
        '''
    )

    def avalia_temp(**context):
        number = float(context['ti'].xcom_pull(task_ids='get_data', key='temperature'))

        if number >= 24:
            return 'group_check_temp.send_email_allert'
        else:
            return 'group.check_temp.send_email_normal'

    check_temp_branch = BranchPythonOperator(
        task_id='check_temp_branch',
        python_callable=avalia_temp,
        provide_context = True,
        task_group=group_check_temp
    )

    with group_check_temp:
        check_temp_branch >> [send_email_allert, send_email_normal]
    
    with group_database:
        create_table >> insert_data
        
    file_sensor_task >> get_data
    get_data >> group_check_temp
    get_data >> group_database