"""Daily: export yesterday's edits from the Eventhouse, join them with views, rebuild the warehouse with dbt,
then compact and vacuum the Delta tables and roll up the cost log."""

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator
from wikipulse_common import DEFAULT_ARGS, FABRIC_CONN_ID, WORKSPACE_ID, notebook_id, notebook_params

from airflow import DAG

DBT_DIR = os.path.join(os.path.dirname(__file__), "dbt")

with DAG(
    dag_id="wikipulse_daily",
    start_date=datetime(2026, 11, 1),
    schedule="30 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["wikipulse", "batch", "dbt"],
    doc_md=__doc__,
) as dag:
    edits = FabricRunItemOperator(
        task_id="edits_daily",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=notebook_id("nb_edits_daily"),
        job_type="RunNotebook",
        job_params=notebook_params(day="{{ data_interval_start.strftime('%Y-%m-%d') }}"),
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt deps --quiet && dbt build --target prod --profiles-dir .",
        execution_timeout=timedelta(minutes=30),
    )
    maintenance = FabricRunItemOperator(
        task_id="maintenance",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=notebook_id("nb_maintenance"),
        job_type="RunNotebook",
        trigger_rule="all_done",  # compact even if dbt failed
    )
    edits >> dbt_build >> maintenance
