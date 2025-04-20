from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from datetime import date, datetime
import pandas as pd
import duckdb
from dateutil.relativedelta import relativedelta
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
}

csv_path = '/opt/airflow/dags/data/Online_Retail.csv'
duckdb_path = '/opt/airflow/db/online_retail.duckdb'
export_dir = '/opt/airflow/sqlite_export'

with DAG(
    dag_id='etl_pipeline_full_mart',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['etl', 'mart', 'duckdb'],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_csv_file",
        filepath=csv_path,
        poke_interval=10,
        timeout=300,
        mode='poke',
    )

    @task()
    def run_etl():
        df = pd.read_csv(csv_path, encoding='latin1')
        df.drop_duplicates(inplace=True)
        df.dropna(subset=['InvoiceNo', 'StockCode', 'InvoiceDate', 'Quantity', 'UnitPrice'], inplace=True)
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df = df[df['InvoiceDate'].notnull()]
        df['InvoiceDate'] = df['InvoiceDate'].apply(lambda x: x + relativedelta(years=14))
        df = df[df['UnitPrice'] >= 0]
        df['Revenue'] = df['Quantity'] * df['UnitPrice']

        # Build Dimensions
        date_df = df[['InvoiceDate']].drop_duplicates().reset_index(drop=True)
        date_df['DateKey'] = date_df.index + 1
        date_df['FullDate'] = date_df['InvoiceDate'].dt.strftime('%Y-%m-%d')
        date_df['Year'] = date_df['InvoiceDate'].dt.year
        date_df['Month'] = date_df['InvoiceDate'].dt.month
        date_df['Day'] = date_df['InvoiceDate'].dt.day
        date_df['Quarter'] = date_df['InvoiceDate'].dt.quarter
        date_df['DayOfWeek'] = date_df['InvoiceDate'].dt.dayofweek

        product_df = df[['StockCode', 'Description']].drop_duplicates().reset_index(drop=True)
        product_df['ProductKey'] = product_df.index + 1

        customer_df = df[['CustomerID', 'Country']].dropna().drop_duplicates().copy()
        customer_df['CustomerID'] = customer_df['CustomerID'].astype(int).astype(str)
        customer_df = customer_df.reset_index(drop=True)
        customer_df['CustomerKey'] = customer_df.index + 1
        customer_df['EffectiveDate'] = pd.Timestamp.now().date()
        customer_df['EndDate'] = None
        customer_df['IsCurrent'] = True

        invoice_df = df[['InvoiceNo', 'InvoiceDate', 'Country']].drop_duplicates().reset_index(drop=True)
        invoice_df['InvoiceKey'] = invoice_df.index + 1
        invoice_df['InvoiceDate'] = invoice_df['InvoiceDate'].dt.strftime('%Y-%m-%d')

        # Build Fact Table
        df = pd.merge(df, date_df[['InvoiceDate', 'DateKey']], on='InvoiceDate', how='left')
        df = pd.merge(df, product_df[['StockCode', 'Description', 'ProductKey']], on=['StockCode', 'Description'], how='left')
        df['CustomerID'] = df['CustomerID'].astype('float').astype('Int64').astype(str)
        df = pd.merge(df, customer_df[['CustomerID', 'Country', 'CustomerKey']], on=['CustomerID', 'Country'], how='left')
        df = pd.merge(df, invoice_df[['InvoiceNo', 'InvoiceKey']], on='InvoiceNo', how='left')
        df = df[df['CustomerKey'].notnull()].copy()
        df = df.reset_index(drop=True)
        df['SalesKey'] = df.index + 1
        fact_df = df[['SalesKey', 'DateKey', 'ProductKey', 'CustomerKey', 'InvoiceKey', 'Quantity', 'UnitPrice', 'Revenue']]

        # Operational Mart
        customer_op = df[['CustomerID', 'Country', 'InvoiceDate']].drop_duplicates().copy()
        customer_op.rename(columns={'InvoiceDate': 'CreatedDate'}, inplace=True)
        product_op = df[['StockCode', 'Description']].drop_duplicates().copy()
        invoice_op = df[['InvoiceNo', 'CustomerID', 'InvoiceDate', 'Country']].drop_duplicates().copy()
        invoice_item_op = df[['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice', 'Revenue']].copy()

        # Save to DuckDB
        con = duckdb.connect(duckdb_path)
        con.execute("CREATE SCHEMA IF NOT EXISTS retail")

        con.execute("CREATE OR REPLACE TABLE retail.DateDimension AS SELECT * FROM date_df")
        con.execute("CREATE OR REPLACE TABLE retail.ProductDimension AS SELECT * FROM product_df")
        con.execute("CREATE OR REPLACE TABLE retail.CustomerDimension AS SELECT * FROM customer_df")
        con.execute("CREATE OR REPLACE TABLE retail.InvoiceDimension AS SELECT * FROM invoice_df")
        con.execute("CREATE OR REPLACE TABLE retail.FactSales AS SELECT * FROM fact_df")

        con.execute("CREATE OR REPLACE TABLE retail.Customer AS SELECT * FROM customer_op")
        con.execute("CREATE OR REPLACE TABLE retail.Product AS SELECT * FROM product_op")
        con.execute("CREATE OR REPLACE TABLE retail.Invoice AS SELECT * FROM invoice_op")
        con.execute("CREATE OR REPLACE TABLE retail.InvoiceItem AS SELECT * FROM invoice_item_op")

        # KPI Metrics
        today = date.today().strftime('%Y-%m-%d')
        now = datetime.now()

        kpi_queries = {
            "TotalRevenueToday": f"""
                SELECT SUM(Revenue) FROM retail.FactSales fs
                JOIN retail.DateDimension dd ON fs.DateKey = dd.DateKey
                WHERE dd.FullDate = '{today}'
            """,
            "TotalQuantityToday": f"""
                SELECT SUM(Quantity) FROM retail.FactSales fs
                JOIN retail.DateDimension dd ON fs.DateKey = dd.DateKey
                WHERE dd.FullDate = '{today}'
            """,
            "UniqueCustomersToday": f"""
                SELECT COUNT(DISTINCT CustomerKey) FROM retail.FactSales fs
                JOIN retail.DateDimension dd ON fs.DateKey = dd.DateKey
                WHERE dd.FullDate = '{today}'
            """,
            "AverageOrderValue": f"""
                SELECT 
                    SUM(Revenue) / NULLIF(COUNT(DISTINCT InvoiceKey), 0)
                FROM retail.FactSales fs
                JOIN retail.DateDimension dd ON fs.DateKey = dd.DateKey
                WHERE dd.FullDate = '{today}'
            """
        }

        con.execute("""
            CREATE OR REPLACE TABLE retail.KPIMetrics (
                MetricName VARCHAR,
                Value DOUBLE,
                MetricDate DATE,
                MetricTimestamp TIMESTAMP
            )
        """)

        for metric, query in kpi_queries.items():
            value = con.execute(query).fetchone()[0] or 0.0
            con.execute("""
                INSERT INTO retail.KPIMetrics (MetricName, Value, MetricDate, MetricTimestamp)
                VALUES (?, ?, ?, ?)
            """, (metric, float(value), today, now))

        # Export to CSV
        os.makedirs(export_dir, exist_ok=True)
        tables = [
            "DateDimension", "ProductDimension", "CustomerDimension", "InvoiceDimension", "FactSales",
            "Customer", "Product", "Invoice", "InvoiceItem", "KPIMetrics"
        ]
        for table in tables:
            export_path = f"{export_dir}/{table}.csv"
            con.execute(f"COPY retail.{table} TO '{export_path}' (HEADER, DELIMITER ',');")

        con.close()

    wait_for_file >> run_etl()
