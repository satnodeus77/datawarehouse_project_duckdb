# Failure Recovery Strategy

## Retry Handling

- Each task in the DAG uses `retries=2`
- Airflow automatically retries tasks that fail

## Error Logging

- Logs are accessible via the Airflow UI per task
- Failures include traceback and helpful messages

## Manual Rerun

- Failed tasks can be manually retried via the UI or CLI
- Historical dates can be rerun using `airflow dags test` for backfilling