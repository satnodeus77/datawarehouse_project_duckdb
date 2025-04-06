# Scheduling Strategy

- **Schedule**: Daily at 2 AM (`0 2 * * *`)
- **Reasoning**:
  - Batch data ingestion is sufficient for retail data
  - 2 AM avoids peak system usage
  - Gives buffer time for data from previous day to arrive

**Partitioning**:
- Logical time partitioning via `InvoiceDate` and `DateDimension`
- Star schema enables time-based aggregation and analysis