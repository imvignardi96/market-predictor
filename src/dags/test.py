from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to be used as a task
def print_hello():
    print("Hello, Airflow!")

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'simple_test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,  # No schedule, will run manually
) as dag:

    # Define task 1
    task1 = PythonOperator(
        task_id='task1',
        python_callable=print_hello,
    )

    # Define task 2
    task2 = PythonOperator(
        task_id='task2',
        python_callable=print_hello,
    )

    # Define task sequence
    task1 >> task2  # task2 will run after task1
