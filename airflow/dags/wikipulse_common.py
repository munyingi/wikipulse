"""Shared settings for the Wikipulse DAGs running in a Fabric Apache Airflow job.

Item IDs come from environment variables set by Terraform, so the same DAG files run in dev and prod.
The Airflow connection ``fabric_wikipulse`` holds the Entra ID app used to call the Fabric job API
(see docs/setup.md).
"""

from __future__ import annotations

import json
import os
from datetime import timedelta

FABRIC_CONN_ID = "fabric_wikipulse"
WORKSPACE_ID = os.environ.get("WIKIPULSE_WORKSPACE_ID", "00000000-0000-0000-0000-000000000000")
ENVIRONMENT = os.environ.get("WIKIPULSE_ENV", "dev")
ALERT_URL = os.environ.get("WIKIPULSE_ALERT_URL", "")


def notebook_id(name: str) -> str:
    return os.environ.get(f"WIKIPULSE_{name.upper()}_ID", "00000000-0000-0000-0000-000000000000")


def notebook_params(**params) -> dict:
    """Fabric job API format for notebook parameters."""
    return {k: {"value": v, "type": "string" if isinstance(v, str) else "float"} for k, v in params.items()}


def alert_on_failure(context) -> None:
    """Post a short message to a Teams or Slack incoming webhook. Never raises."""
    if not ALERT_URL:
        return
    import requests

    ti = context["task_instance"]
    text = (
        f"Wikipulse {ENVIRONMENT}: task {ti.dag_id}.{ti.task_id} failed "
        f"for {context.get('logical_date')} (try {ti.try_number}). Log: {ti.log_url}"
    )
    try:
        requests.post(
            ALERT_URL, data=json.dumps({"text": text}), headers={"Content-Type": "application/json"}, timeout=10
        )
    except Exception:  # alerting must never break the task's own error handling
        pass


DEFAULT_ARGS = {
    "owner": "wikipulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}
