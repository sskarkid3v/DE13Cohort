from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract(**context):
    batch_id="batch_20250101_001"
    
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    
    return batch_id

def transform(**context):
    ti = context["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_task")
    
    batch_id_from_return = ti.xcom_pull(task_ids="extract_task", key="return_value")
    
    print("batch_id from xcom_push:", batch_id)
    print("batch_id from return_value:", batch_id_from_return)
    
    output_path = f"/tmp/{batch_id_from_return}_transformed.csv"
    ti.xcom_push(key="output_path", value=output_path)

def load(**context):
    ti = context["ti"]
    output_path = ti.xcom_pull(key="output_path", task_ids="transform_task")
    
    print(f"Loading data from {output_path} into the database...")
    
with DAG(
    dag_id="xcom_demo",
    start_date=datetime(2024, 1, 20),
    schedule=None,
    catchup=False,
    tags=["xcom", "demo"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task

    