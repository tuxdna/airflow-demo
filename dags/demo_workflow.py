from datetime import datetime, timedelta
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from helpers import *

# 1. Define default arguments for scheduling and retries
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Initialize the DAG
with DAG(
    'airflow_capability_demo',
    default_args=default_args,
    description='A DAG demonstrating core Airflow features',
    schedule='@daily',
    catchup=False,
    tags=['demo', 'e-commerce'],
) as dag:

    # Feature A: PythonOperator & XComs (Data Passing)
    task_fetch_data = PythonOperator(
        task_id='fetch_sales_data',
        python_callable=fetch_sales_data,
    )

    # Feature B: Conditional Branching
    task_branch = BranchPythonOperator(
        task_id='validate_data_quality',
        python_callable=check_data_quality,
    )

    # Feature C: Parallel Execution (Independent Workers)
    task_process_us = PythonOperator(
        task_id='process_us_revenue',
        python_callable=process_us,
    )

    task_process_eu = PythonOperator(
        task_id='process_eu_revenue',
        python_callable=process_eu,
    )

    task_halt = EmptyOperator(
        task_id='halt_pipeline',
    )

    # Feature D: Trigger Rules (Joining paths)
    task_notify = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_notification,
        # 'all_done' ensures this runs even if some upstream branches were skipped
        trigger_rule='all_done', 
    )

    # 3. Define the Dependency Graph (The Pipeline Layout)
    task_fetch_data >> task_branch
    
    # Branching paths
    task_branch >> [task_process_us, task_halt]
    task_branch >> task_process_eu  # Branch to EU as well
    
    # Re-converging paths
    [task_process_us, task_process_eu, task_halt] >> task_notify
