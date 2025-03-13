from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_context(**kwargs):
    """Prints the full execution context provided by Airflow."""
    for key, value in kwargs.items():
        print(f"{key}: {value}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 12),
}

with DAG(
    dag_id='print_context_dag',
    default_args=default_args,
    start_date=datetime(year=2019, month=1, day=1),
    end_date=datetime(year=2019, month=1, day=5),
    schedule="@daily",
) as dag:

    print_task = PythonOperator(
        task_id='print_execution_context',
        python_callable=print_context,
        provide_context=True,  # Ensures Airflow passes DAG execution details
    )

    print_task
