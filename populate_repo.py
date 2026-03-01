import os

# 1. Define the 10-pipeline structure
files = {
    # AIRFLOW ORCHESTRATION
    "dags/ecom_main_dag.py": """
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('ecom_pipeline', start_date=datetime(2026, 1, 1), schedule='@daily', catchup=False) as dag:
    run_dbt = BashOperator(task_id='dbt_run', bash_command='dbt run')
    test_dbt = BashOperator(task_id='dbt_test', bash_command='dbt test')
    run_dbt >> test_dbt
""",
    # SILVER LAYER (Staging)
    "dbt/models/staging/stg_customers.sql": "select id as customer_id, first_name, last_name, email from raw.customers",
    "dbt/models/staging/stg_orders.sql": "select id as order_id, user_id as customer_id, order_date, status from raw.orders",
    "dbt/models/staging/stg_payments.sql": "select id as payment_id, order_id, payment_method, amount / 100.0 as amount_usd from raw.payments",
    
    # LOGIC LAYER (Intermediate)
    "dbt/models/intermediate/int_orders_joined.sql": "select o.*, p.amount_usd, p.payment_method from {{ ref('stg_orders') }} o left join {{ ref('stg_payments') }} p on o.order_id = p.order_id",
    "dbt/models/intermediate/int_customer_history.sql": "select customer_id, min(order_date) as first_order, count(order_id) as total_orders from {{ ref('stg_orders') }} group by 1",
    "dbt/models/intermediate/int_daily_revenue.sql": "select date_trunc('day', order_date) as date, sum(amount_usd) as revenue from {{ ref('int_orders_joined') }} group by 1",

    # GOLD LAYER (Marts)
    "dbt/models/marts/fct_orders.sql": "select * from {{ ref('int_orders_joined') }}",
    "dbt/models/marts/dim_customers.sql": "select c.*, h.first_order, h.total_orders from {{ ref('stg_customers') }} c left join {{ ref('int_customer_history') }} h on c.customer_id = h.customer_id",
    "dbt/models/marts/fct_customer_ltv.sql": "select customer_id, sum(amount_usd) as ltv from {{ ref('int_orders_joined') }} group by 1",
    "dbt/models/marts/dim_product_performance.sql": "select category, count(*) as sales_count from {{ ref('stg_products') }} group by 1"
}

# 2. Write the files safely
for path, content in files.items():
    full_path = os.path.join(os.getcwd(), path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w") as f:
        f.write(content.strip())
    print(f"Successfully wrote: {path}")

print("\n--- Playground Ready ---")