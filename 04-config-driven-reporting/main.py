# Airflow DAG for the finance reports, built from config.yaml.
# Keep the words "airflow" and "dag" in this file: Airflow skips files without them.
"""One BigQuery task per script. graph.py decides the arrows between them."""

import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from graph import HERE, build_graph, build_order, load_config, reports_using

config = load_config()
graph = build_graph(config)
counts = reports_using(graph)

with DAG(
    dag_id=config["dag_id"],
    # A data-interval schedule makes each run cover the day that just ended:
    # the 6 Nov run has {{ data_interval_start | ds }} = 2026-11-05.
    schedule=CronDataIntervalTimetable(config["schedule"], timezone="UTC"),
    start_date=pendulum.instance(pendulum.parse(str(config["start_date"]))),
    catchup=False,
    params=config.get("params", {}),
) as dag:
    tasks = {}
    for node in build_order(graph):
        layer, name = node.split(".")
        tasks[node] = BigQueryInsertJobOperator(
            task_id=node,
            configuration={
                "query": {
                    "query": (HERE / layer / f"{name}.sql").read_text(),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=config["gcp_conn_id"],
            location=config["location"],
            # When several tasks are ready, the one more reports need runs first.
            priority_weight=counts[node],
            weight_rule="absolute",
        )

    for node, needs in graph.items():
        for need in needs:
            tasks[need] >> tasks[node]
