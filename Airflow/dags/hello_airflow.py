from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="hello_airflow",
    default_args=default_args,
    description="A simple hello world DAG",
    start_date=datetime(2024, 1, 20),
    schedule=None,
    catchup=False,
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command='echo "START: Airflow is running"',
    )
    
    echo_date = BashOperator(
        task_id="echo_date",
        bash_command="this_command_does_not_exist",  # This will intentionally fail
    )
    
    end = BashOperator(
        task_id="end",
        bash_command='echo "END: Airflow DAG completed"',
    )
    
    start >> echo_date >> end