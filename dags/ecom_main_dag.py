from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('ecom_pipeline', start_date=datetime(2026, 1, 1), schedule='@daily', catchup=False) as dag:
    run_dbt = BashOperator(task_id='dbt_run', bash_command='dbt run')
    test_dbt = BashOperator(task_id='dbt_test', bash_command='dbt test')
    run_dbt >> test_dbt