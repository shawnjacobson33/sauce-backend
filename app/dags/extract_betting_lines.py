from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv


load_dotenv()


def extract_func():
    print("extracted")


def transform_func():
    print("transformed")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 14),
    'retries': 3,
    'retry_exponential_backoff': True
}


with DAG(
    dag_id='betting_lines',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5)
) as dag:


    extract_task = PythonOperator(
        task_id='extracting PrizePicks lines',
        python_callable=extract_func,
    )


    transform_task = PythonOperator(
        task_id='transform PrizePicks lines',
        python_callable=transform_func,
    )

    extract_task >> transform_task