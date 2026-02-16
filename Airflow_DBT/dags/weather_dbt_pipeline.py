from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from tasks.extract_weather import extract_and_load_weather

with DAG(
    dag_id="weather_dbt_pipeline",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False,
    tags=["weather", "dbt", "demo"],
) as dag:
    
    init_warehouse = BashOperator(
        task_id="init_warehouse",
        bash_command="""
        psql -h $WAREHOUSE_HOST -p $WAREHOUSE_PORT -U $WAREHOUSE_USER -d $WAREHOUSE_DB  \
            -f /opt/airflow/dbt/init_warehouse.sql
        """,
    )
    
    extract_load = PythonOperator(
        task_id="extract_load_weather_to_bronze",
        python_callable=extract_and_load_weather,
    )
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd $DBT_PROJECT_DIR && \
        dbt run --profiles-dir $DBT_PROFILES_DIR
        """,
    )
    
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd $DBT_PROJECT_DIR && \
        dbt test --profiles-dir $DBT_PROFILES_DIR
        """,
    )
    
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="""
        cd $DBT_PROJECT_DIR && \
        dbt docs generate --profiles-dir $DBT_PROFILES_DIR
        """,
    )
    
    init_warehouse >> extract_load >> dbt_run >> dbt_test >> dbt_docs