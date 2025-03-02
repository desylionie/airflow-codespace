from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello World!'

dag = DAG('hello_world_desy', description = 'Simple Tutorial DAG',
          schedule_interval='@daily',
          start_date=datetime(2025,1,1),
          catchup=False, #kalau detect error ga bakal jalan
          tags=['hello_world', 'desy']
        )

task_hello = PythonOperator(
    task_id='print_hello', 
    python_callable=print_hello, 
    dag=dag)

task_hello