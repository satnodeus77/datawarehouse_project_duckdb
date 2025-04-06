# DAG Architecture

This project has two DAGs:

## 1. `etl_pipeline_full` DAG

```mermaid
graph TD
    Extract --> Transform --> Load
```

- **Extract**: Load and clean CSV data
- **Transform**: Build dimension tables and fact table (star schema)
- **Load**: Load all transformed tables into DuckDB

## 2. `data_quality_check` DAG

```mermaid
graph TD
    Check_Null_CustomerID --> Check_Negative_UnitPrice
```

- **Check_Null_CustomerID**: Verifies no NULLs in `CustomerKey`
- **Check_Negative_UnitPrice**: Ensures no negative unit prices in FactSales