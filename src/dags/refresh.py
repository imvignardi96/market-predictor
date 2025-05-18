from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='test_dummy_dag',
    default_args=default_args,
    description='A simple test DAG with a dummy task',
    schedule='@daily',
    catchup=False,
    tags=['test'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> end
