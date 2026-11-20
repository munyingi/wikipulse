"""Hourly: run the producer notebook for 55 minutes, streaming Wikimedia recent changes into the Eventstream.
Needs the Key Vault secret described in docs/setup.md. Pause this DAG to stop the live feed."""

from datetime import datetime, timedelta

from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator
from wikipulse_common import DEFAULT_ARGS, FABRIC_CONN_ID, WORKSPACE_ID, notebook_id, notebook_params

from airflow import DAG

with DAG(
    dag_id="wikipulse_stream_producer",
    start_date=datetime(2026, 11, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["wikipulse", "streaming"],
    doc_md=__doc__,
) as dag:
    FabricRunItemOperator(
        task_id="stream_recent_changes",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=notebook_id("nb_stream_producer"),
        job_type="RunNotebook",
        job_params=notebook_params(minutes=55.0),
        execution_timeout=timedelta(minutes=58),
    )
