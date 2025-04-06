# ETL Pipeline with Apache Airflow & DuckDB

This project implements an ETL pipeline for retail sales data using Apache Airflow and DuckDB. It is containerized using Docker and includes:

- Data cleaning and transformation
- Star schema construction (Date, Product, Customer, Invoice dimensions)
- Fact table creation (FactSales)
- Data quality validation DAG
- Daily scheduling at 2 AM