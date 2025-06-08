from airflow.decorators import task, dag
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),  # Set to your desired start date
    'retries': 1,
}

# Define the DAG using the decorator
@dag(
    default_args=default_args,
    schedule='0 22 * * 6',  # This cron expression means 22:00 on Saturdays
    catchup=False,  # To avoid running missed instances
    tags=['test'],
)
def send_test_email():
    @task()
    def send_email():
        return EmailOperator(
            task_id='send_test_email_task',
            to='natiolo21@gmail.com',
            subject='Test Email',
            html_content='<h3>This is a test email sent by Airflow!</h3>',
        ).execute(context={})

    send_email()

# Instantiate the DAG
test_email_dag = send_test_email()
