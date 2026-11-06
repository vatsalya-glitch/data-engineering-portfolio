# Airflow DAG for the finance reports, built from config.yaml.
# Keep the words "airflow" and "dag" in this file: Airflow skips files without them.
"""The finance reports DAG.

One task per SQL script (ingestion, transformation, report). graph.py works
out which task needs which; this file only turns that into Airflow tasks and
arrows.
"""

import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from graph import HERE, build_graph, build_order, load_config, reports_using

# ---------------------------------------------------------------------------
# DAG settings. These belong to the DAG, not to the reports, so they live
# here and config.yaml holds only the reports and their steps.
# ---------------------------------------------------------------------------
DAG_ID = "finance_reports"
SCHEDULE = "0 6 * * *"                       # every day at 06:00, for the day that just ended
START_DATE = pendulum.datetime(2026, 11, 1, tz="UTC")
GCP_CONN_ID = "google_cloud_default"         # Airflow connection to Google Cloud
LOCATION = "US"                              # BigQuery location of the datasets
# Values the SQL scripts can use as {{ params.<name> }}.
PARAMS = {
    "source_connection": "my-gcp-project.us.shop_db",   # BigQuery connection to the shop database
}

# Read the config and build the graph. If the config has a mistake,
# build_graph raises an error and Airflow shows it as an import error
# for this DAG, so nothing runs with a broken config.
config = load_config()
graph = build_graph(config)        # node -> nodes it needs
counts = reports_using(graph)      # node -> how many reports need it

with DAG(
    dag_id=DAG_ID,
    # A data-interval schedule makes each run cover the day that just ended:
    # the run at 06:00 on 6 Nov has {{ data_interval_start | ds }} = 2026-11-05.
    # (With a plain cron string, Airflow 3 would give the current day instead.)
    schedule=CronDataIntervalTimetable(SCHEDULE, timezone="UTC"),
    start_date=START_DATE,
    catchup=False,                 # don't run all the past days when the DAG is turned on
    params=PARAMS,                 # available in the SQL as {{ params.<name> }}
) as dag:

    # 1. One task per node, created in build order. The task id is the node
    #    name, e.g. "ingestion.orders", and the task runs <layer>/<name>.sql.
    tasks = {}
    for node in build_order(graph):
        layer, name = node.split(".")
        tasks[node] = BigQueryInsertJobOperator(
            task_id=node,
            configuration={
                "query": {
                    # Airflow fills in the {{ ... }} templates in the script before running it.
                    "query": (HERE / layer / f"{name}.sql").read_text(),
                    "useLegacySql": False,     # BigQuery hooks default to legacy SQL
                }
            },
            gcp_conn_id=GCP_CONN_ID,
            location=LOCATION,
            # When several tasks are ready at the same time, Airflow starts the
            # one with the highest priority_weight first. Using the number of
            # reports that need the task makes the most-shared work go first.
            # "absolute" means: use this number as is, don't add up downstream weights.
            priority_weight=counts[node],
            weight_rule="absolute",
        )

    # 2. One arrow per dependency: a task starts only after every task it needs
    #    has succeeded. If one fails, only the tasks that need it stop; the
    #    other reports carry on.
    for node, needs in graph.items():
        for need in needs:
            tasks[need] >> tasks[node]
