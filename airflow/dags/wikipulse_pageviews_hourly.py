"""Hourly page views: download the hour's dump, then load bronze -> silver -> gold.

Runs at :50. The run whose interval starts at 13:50 processes the 13:00-14:00 file, which Wikimedia publishes
shortly after 14:00. If the file is late, the download task fails with "not published yet" and retries.
"""

from datetime import datetime, timedelta

from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator
from wikipulse_common import DEFAULT_ARGS, FABRIC_CONN_ID, WORKSPACE_ID, notebook_id, notebook_params

from airflow import DAG

HOUR = "{{ data_interval_start.strftime('%Y-%m-%dT%H:00:00Z') }}"

with DAG(
    dag_id="wikipulse_pageviews_hourly",
    start_date=datetime(2026, 11, 1),
    schedule="50 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["wikipulse", "batch"],
    doc_md=__doc__,
) as dag:
    download = FabricRunItemOperator(
        task_id="download_hour",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=notebook_id("nb_download_hour"),
        job_type="RunNotebook",
        job_params=notebook_params(hour=HOUR),
        retries=6,
        retry_delay=timedelta(minutes=10),
        check_interval=30,
    )
    process = FabricRunItemOperator(
        task_id="process_hour",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=notebook_id("nb_process_hour"),
        job_type="RunNotebook",
        job_params=notebook_params(hour=HOUR),
        execution_timeout=timedelta(minutes=45),
        check_interval=30,
    )
    download >> process
