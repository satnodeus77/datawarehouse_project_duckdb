from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import pandas as pd
import duckdb

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
}

csv_path = '/opt/airflow/dags/data/Online_Retail.csv'
db_path = '/opt/airflow/db/online_retail.duckdb'

with DAG(
    dag_id='etl_pipeline_full',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['etl', 'duckdb', 'star-schema'],
) as dag:

    # ✅ 1. FileSensor waits for CSV to exist
    wait_for_file = FileSensor(
        task_id="wait_for_csv_file",
        filepath=csv_path,
        poke_interval=10,
        timeout=300,
        mode='poke',
    )

    # ✅ 2. Extraction task
    @task()
    def extract():
        df = pd.read_csv(csv_path, encoding='latin1')
        df.drop_duplicates(inplace=True)
        df.dropna(subset=['InvoiceNo', 'StockCode', 'InvoiceDate', 'Quantity', 'UnitPrice'], inplace=True)
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df = df[df['InvoiceDate'].notnull()]
        df = df[df['UnitPrice'] >= 0]
        df['Revenue'] = df['Quantity'] * df['UnitPrice']
        return df.to_json(orient='split')

    @task()
    def transform(df_json):
        df = pd.read_json(df_json, orient='split')
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

        date_df = df[['InvoiceDate']].drop_duplicates().reset_index(drop=True)
        date_df['DateKey'] = date_df.index + 1
        date_df['FullDate'] = date_df['InvoiceDate'].dt.date
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
        invoice_df['InvoiceDate'] = invoice_df['InvoiceDate'].dt.date

        df = pd.merge(df, date_df[['InvoiceDate', 'DateKey']], on='InvoiceDate', how='left')
        df = pd.merge(df, product_df[['StockCode', 'Description', 'ProductKey']], on=['StockCode', 'Description'], how='left')
        df['CustomerID'] = df['CustomerID'].astype('float').astype('Int64').astype(str)
        df = pd.merge(df, customer_df[['CustomerID', 'Country', 'CustomerKey']], on=['CustomerID', 'Country'], how='left')
        df = pd.merge(df, invoice_df[['InvoiceNo', 'InvoiceKey']], on='InvoiceNo', how='left')

        unmatched = df[df['CustomerKey'].isnull()]
        if not unmatched.empty:
            print("⚠️ Unmatched customers found:")
            print(unmatched[['CustomerID', 'Country']].drop_duplicates().head(10))

        df = df[df['CustomerKey'].notnull()].copy()
        df = df.reset_index(drop=True)
        df['SalesKey'] = df.index + 1
        fact_df = df[['SalesKey', 'DateKey', 'ProductKey', 'CustomerKey', 'InvoiceKey', 'Quantity', 'UnitPrice', 'Revenue']]

        return {
            "date": date_df.to_json(orient='split'),
            "product": product_df.to_json(orient='split'),
            "customer": customer_df.to_json(orient='split'),
            "invoice": invoice_df.to_json(orient='split'),
            "fact": fact_df.to_json(orient='split')
        }

    @task()
    def load(schema):
        con = duckdb.connect(db_path)
        con.execute("CREATE SCHEMA IF NOT EXISTS retail")
        for name, df_json in schema.items():
            df = pd.read_json(df_json, orient='split')
            table_name = f"{name}_dim" if name != "fact" else "fact_sales"
            con.execute(f"CREATE OR REPLACE TABLE retail.{table_name} AS SELECT * FROM df")
        con.close()

    # ✅ 3. Set task dependencies
    wait_for_file >> load(transform(extract()))
