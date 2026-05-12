from datetime import datetime, timedelta
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

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
    def fetch_sales_data(**kwargs):
        # Simulating data ingestion
        sales_amount = 15000
        print(f"Fetched sales total: ${sales_amount}")
        # Returning a value automatically stores it in XCom
        return sales_amount

    task_fetch_data = PythonOperator(
        task_id='fetch_sales_data',
        python_callable=fetch_sales_data,
    )

    # Feature B: Conditional Branching
    def check_data_quality(**kwargs):
        # Pull data from the previous task using XCom
        ti = kwargs['ti']
        fetched_amount = ti.xcom_pull(task_ids='fetch_sales_data')
        
        if fetched_amount > 0:
            return 'process_us_revenue' # Proceed to parallel processing
        else:
            return 'halt_pipeline'      # Divert to empty stop task

    task_branch = BranchPythonOperator(
        task_id='validate_data_quality',
        python_callable=check_data_quality,
    )

    # Feature C: Parallel Execution (Independent Workers)
    def process_us(**kwargs):
        print("Processing North American region data...")

    def process_eu(**kwargs):
        print("Processing European region data...")

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
    def send_notification():
        print("Pipeline completed successfully. Alerting team.")

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
