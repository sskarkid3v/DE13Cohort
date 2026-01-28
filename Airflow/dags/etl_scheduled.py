from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="scheduled_etl",
    default_args=default_args,
    description="A simple hello world DAG",
    start_date=datetime(2025, 1, 20),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["scheduled", "etl"],
) as dag:

    extract = BashOperator(
        task_id="start",
        bash_command='echo "raw_data=hello" > /tmp/raw.txt',
    )
    
    transform = BashOperator(
        task_id="echo_date",
        bash_command="cat /tmp/raw.txt | sed \"s/hello/hello_airflow/\" > /tmp/transformed.txt",  # This will intentionally fail
    )
    
    load = BashOperator(
        task_id="end",
        bash_command='echo "loaded:" && cat /tmp/transformed.txt',
    )

    extract >> transform >> load