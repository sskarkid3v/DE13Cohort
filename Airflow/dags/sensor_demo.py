from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="sensor_demo",
    default_args=default_args,
    description="A DAG demonstrating the FileSensor",
    start_date=datetime(2024, 1, 20),
    schedule=None,
    catchup=False,
    tags=["sensor", "demo"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/dags/data/data_ready.txt",
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )
    
    process_file = BashOperator(
        task_id="process_file",
        bash_command='echo "Processing file /opt/airflow/dags/data_ready.txt"',
    )
    

    wait_for_file >> process_file
