# airflow DAG: built from configs/daily_operations.yaml by the report framework.
# Keep this comment: Airflow only parses files containing the words "airflow" and "dag".
from report_framework import build_dag

dag = build_dag("configs/daily_operations.yaml")
