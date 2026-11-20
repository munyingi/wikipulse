"""Deploys what Terraform does not: the Python package and the Airflow DAGs.

1. Zips ``src/wikipulse`` and uploads it to the lakehouse at ``Files/libs/wikipulse.zip``
   (the notebooks add it to ``sys.path``).
2. Uploads ``airflow/dags`` and the dbt project into the Apache Airflow job, keeping the job settings
   Terraform manages.

Auth uses ``DefaultAzureCredential``: ``az login`` locally, or the AZURE_* / OIDC variables in CI.

    python scripts/deploy.py --env dev            # reads IDs from `terraform output`
    python scripts/deploy.py --env dev --dry-run  # build and list what would be uploaded
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import subprocess
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FABRIC_API = "https://api.fabric.microsoft.com/v1"
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"
DAG_DIR = ROOT / "airflow" / "dags"
DBT_DIR = ROOT / "dbt"
DBT_SKIP = {"target", "logs", "dbt_packages", ".user.yml"}


def build_package_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted((ROOT / "src" / "wikipulse").rglob("*.py")):
            zf.write(path, path.relative_to(ROOT / "src").as_posix())
    return buf.getvalue()


def airflow_files() -> dict[str, bytes]:
    """Definition paths -> content for the Airflow job: DAGs plus the dbt project under dags/dbt/."""
    files = {f"dags/{p.name}": p.read_bytes() for p in sorted(DAG_DIR.glob("*.py"))}
    for p in sorted(DBT_DIR.rglob("*")):
        rel = p.relative_to(DBT_DIR)
        if p.is_file() and not (set(rel.parts) & DBT_SKIP):
            files[f"dags/dbt/{rel.as_posix()}"] = p.read_bytes()
    return files


def merge_definition(current_parts: list[dict], files: dict[str, bytes]) -> list[dict]:
    """Keep every non-DAG part (job settings managed by Terraform) and replace all dags/ parts."""
    kept = [p for p in current_parts if not p["path"].startswith("dags/")]
    new = [
        {"path": path, "payload": base64.b64encode(data).decode(), "payloadType": "InlineBase64"}
        for path, data in files.items()
    ]
    return kept + new


def terraform_outputs(env: str) -> dict:
    out = subprocess.run(
        ["terraform", f"-chdir={ROOT / 'terraform'}", "output", "-json"],
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "TF_WORKSPACE": env},
    )
    return {k: v["value"] for k, v in json.loads(out.stdout).items()}


class Fabric:
    def __init__(self):
        from azure.identity import DefaultAzureCredential

        self.cred = DefaultAzureCredential()

    def _headers(self) -> dict:
        token = self.cred.get_token("https://api.fabric.microsoft.com/.default").token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _lro(self, resp):
        import requests

        if resp.status_code != 202:
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        url = resp.headers["Location"]
        while True:
            time.sleep(int(resp.headers.get("Retry-After", 3)))
            resp = requests.get(url, headers=self._headers(), timeout=60)
            resp.raise_for_status()
            state = resp.json().get("status")
            if state in ("Succeeded", "Failed"):
                if state == "Failed":
                    raise RuntimeError(resp.json())
                result = requests.get(url + "/result", headers=self._headers(), timeout=60)
                return result.json() if result.ok and result.content else {}

    def get_definition(self, workspace_id: str, item_id: str) -> list[dict]:
        import requests

        r = requests.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/getDefinition", headers=self._headers(), timeout=60
        )
        return self._lro(r)["definition"]["parts"]

    def update_definition(self, workspace_id: str, item_id: str, parts: list[dict]) -> None:
        import requests

        r = requests.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
            headers=self._headers(),
            json={"definition": {"parts": parts}},
            timeout=120,
        )
        self._lro(r)

    def find_item(self, workspace_id: str, item_type: str, name: str) -> str:
        import requests

        r = requests.get(
            f"{FABRIC_API}/workspaces/{workspace_id}/items?type={item_type}", headers=self._headers(), timeout=60
        )
        r.raise_for_status()
        for item in r.json()["value"]:
            if item["displayName"] == name:
                return item["id"]
        raise LookupError(f"{item_type} {name!r} not found")

    def upload_file(self, workspace_id: str, lakehouse_id: str, path: str, data: bytes) -> None:
        from azure.storage.filedatalake import DataLakeServiceClient

        svc = DataLakeServiceClient(ONELAKE, credential=self.cred)
        fs = svc.get_file_system_client(workspace_id)
        fs.get_file_client(f"{lakehouse_id}/Files/{path}").upload_data(data, overwrite=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev")
    ap.add_argument("--workspace-id")
    ap.add_argument("--lakehouse-id")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    pkg = build_package_zip()
    files = airflow_files()
    print(f"package zip: {len(pkg):,} bytes; airflow files: {len(files)}")
    if a.dry_run:
        for path in files:
            print("  ", path)
        return

    outputs = {} if (a.workspace_id and a.lakehouse_id) else terraform_outputs(a.env)
    ws = a.workspace_id or outputs["workspace_id"]
    lh = a.lakehouse_id or outputs["lakehouse_id"]
    fabric = Fabric()
    fabric.upload_file(ws, lh, "libs/wikipulse.zip", pkg)
    print("uploaded Files/libs/wikipulse.zip")
    try:
        job = fabric.find_item(ws, "ApacheAirflowJob", "af_wikipulse")
    except LookupError:
        print("no Airflow job in this workspace (enable_airflow_job = false); skipping DAG upload")
        return
    parts = merge_definition(fabric.get_definition(ws, job), files)
    fabric.update_definition(ws, job, parts)
    print(f"uploaded {len(files)} files to Airflow job af_wikipulse")


if __name__ == "__main__":
    main()
