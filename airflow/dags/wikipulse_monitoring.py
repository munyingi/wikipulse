"""Every 30 minutes: fail (and alert) if the newest loaded hour is older than the freshness SLA."""

from datetime import datetime, timedelta

from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator
from wikipulse_common import DEFAULT_ARGS, FABRIC_CONN_ID, WORKSPACE_ID, notebook_id, notebook_params

from airflow import DAG

with DAG(
    dag_id="wikipulse_freshness",
    start_date=datetime(2026, 11, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    dagrun_timeout=timedelta(minutes=25),
    tags=["wikipulse", "monitoring"],
    doc_md=__doc__,
) as dag:
    FabricRunItemOperator(
        task_id="check_freshness",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=notebook_id("nb_freshness"),
        job_type="RunNotebook",
        job_params=notebook_params(sla_hours=3.0),
    )
