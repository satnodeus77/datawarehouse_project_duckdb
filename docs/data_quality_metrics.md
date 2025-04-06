# Data Quality Metrics

This project implements two key data quality checks using a separate Airflow DAG (`data_quality_check`).

| Check Description          | Table/Field             | Threshold | Action         |
|---------------------------|-------------------------|-----------|----------------|
| Null CustomerKey values   | `FactSales.CustomerKey` | 0         | Fail DAG       |
| Negative UnitPrice values | `FactSales.UnitPrice`   | 0         | Fail DAG       |

## Implementation Notes

- Checks are implemented using native Python and SQL via DuckDB.
- If any check fails, the corresponding Airflow task raises an error and marks the DAG as failed.
- These checks ensure data integrity for downstream reporting or analytics.