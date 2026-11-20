import base64
import importlib.util
import io
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("deploy", ROOT / "scripts" / "deploy.py")
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


def test_package_zip_is_importable(tmp_path):
    data = deploy.build_package_zip()
    names = zipfile.ZipFile(io.BytesIO(data)).namelist()
    assert "wikipulse/__init__.py" in names and "wikipulse/pipeline.py" in names
    zpath = tmp_path / "wikipulse.zip"
    zpath.write_bytes(data)
    import subprocess
    import sys

    out = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            f"import sys; sys.path.insert(0, {str(zpath)!r}); import wikipulse.ops as o; print(o.__file__)",
        ],
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0 and "wikipulse.zip" in out.stdout


def test_airflow_files_include_dags_and_dbt_but_not_build_output():
    files = deploy.airflow_files()
    assert "dags/wikipulse_pageviews_hourly.py" in files
    assert "dags/dbt/dbt_project.yml" in files
    assert not [p for p in files if "/target/" in p or "/logs/" in p]


def test_merge_definition_keeps_job_settings_and_replaces_dags():
    current = [
        {"path": "apacheairflowjob-content.json", "payload": "e30=", "payloadType": "InlineBase64"},
        {"path": "dags/old_dag.py", "payload": "", "payloadType": "InlineBase64"},
    ]
    parts = deploy.merge_definition(current, {"dags/new.py": b"print(1)"})
    assert [p["path"] for p in parts] == ["apacheairflowjob-content.json", "dags/new.py"]
    assert base64.b64decode(parts[1]["payload"]) == b"print(1)"
