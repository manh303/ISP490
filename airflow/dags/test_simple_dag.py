from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello from Tiki Lazada test DAG!")
    return "Success"

dag = DAG(
    'test_tiki_lazada_simple',
    description='Simple test DAG',
    schedule_interval=None,
    start_date=datetime(2025, 11, 6),
    catchup=False,
    tags=['test', 'tiki', 'lazada']
)

test_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag
)