def fetch_sales_data(**kwargs):
    # Simulating data ingestion
    sales_amount = 15000
    print(f"Fetched sales total: ${sales_amount}")
    # Returning a value automatically stores it in XCom
    return sales_amount


def check_data_quality(**kwargs):
    # Pull data from the previous task using XCom
    ti = kwargs['ti']
    fetched_amount = ti.xcom_pull(task_ids='fetch_sales_data')
    
    if fetched_amount > 0:
        return 'process_us_revenue' # Proceed to parallel processing
    else:
        return 'halt_pipeline'      # Divert to empty stop task


# Feature C: Parallel Execution (Independent Workers)
def process_us(**kwargs):
    print("Processing North American region data...")


def process_eu(**kwargs):
    print("Processing European region data...")


# Feature D: Trigger Rules (Joining paths)
def send_notification():
    print("Pipeline completed successfully. Alerting team.")
