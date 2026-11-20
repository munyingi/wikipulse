"""Manual backfill: load every hour between `start` and `end` that is not loaded yet.

Trigger with config, for example {"start": "2026-11-18T00:00:00Z", "end": "2026-11-20T00:00:00Z"}.
Already loaded hours are skipped, and re-loading an hour replaces it, so the DAG is safe to run twice.
"""

from datetime import datetime, timedelta

from airflow.models.param import Param
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator
from wikipulse_common import DEFAULT_ARGS, FABRIC_CONN_ID, WORKSPACE_ID, notebook_id, notebook_params

from airflow import DAG

with DAG(
    dag_id="wikipulse_backfill",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"start": Param("", type="string"), "end": Param("", type="string")},
    tags=["wikipulse", "batch", "backfill"],
    doc_md=__doc__,
) as dag:
    FabricRunItemOperator(
        task_id="backfill_missing_hours",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=notebook_id("nb_backfill"),
        job_type="RunNotebook",
        job_params=notebook_params(start="{{ params.start }}", end="{{ params.end }}"),
        execution_timeout=timedelta(hours=6),
    )
