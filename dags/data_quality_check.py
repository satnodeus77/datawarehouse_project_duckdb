from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import duckdb

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

db_path = '/opt/airflow/db/online_retail.duckdb'

with DAG(
    dag_id='data_quality_check',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['quality', 'duckdb'],
) as dag:

    @task()
    def check_null_customer_ids():
        con = duckdb.connect(db_path)
        result = con.execute("SELECT COUNT(*) FROM retail.fact_sales WHERE CustomerKey IS NULL").fetchone()[0]
        con.close()
        if result > 0:
            raise ValueError(f"❌ Found {result} rows with NULL CustomerKey in fact_sales.")
        print("✅ CustomerKey NULL check passed.")

    @task()
    def check_negative_unit_price():
        con = duckdb.connect(db_path)
        result = con.execute("SELECT COUNT(*) FROM retail.fact_sales WHERE UnitPrice < 0").fetchone()[0]
        con.close()
        if result > 0:
            raise ValueError(f"❌ Found {result} rows with negative UnitPrice in fact_sales.")
        print("✅ UnitPrice negative check passed.")

    check_null_customer_ids() >> check_negative_unit_price()
