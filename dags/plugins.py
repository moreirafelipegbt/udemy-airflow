from airflow import DAG
from big_data_operator import BigDataOperator
from datetime import datetime

with DAG('bigdata_operator', description="big Data operator DAG", schedule_interval=None, start_date=datetime(2023,5,29), catchup=False) as dag:

    big_data_operator = BigDataOperator(
        task_id="big_data",
        path_to_csv_file ="/opt/airflow/data/Churn.csv",
        path_to_save_file = "/opt/airflow/data/Churn.parquet",
        file_type="parquet"
        )
    
big_data_operator