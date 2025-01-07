from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import time

# Function to simulate a task
def wait_and_print():
    print("Waiting 5 seconds before printing...")
    time.sleep(5)
    print("Done waiting!")

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'simple_wait_test_dag',
    default_args=default_args,
    description='A simple DAG with wait time',
    schedule_interval=None,  # No schedule, will run manually
) as dag:

    # Start of the DAG
    start_task = DummyOperator(
        task_id='start_task'
    )

    # Task that waits and then prints
    wait_task = PythonOperator(
        task_id='wait_task',
        python_callable=wait_and_print
    )

    # End of the DAG
    end_task = DummyOperator(
        task_id='end_task'
    )

    # Define task dependencies
    start_task >> wait_task >> end_task
