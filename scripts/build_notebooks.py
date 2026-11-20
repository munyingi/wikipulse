"""Builds Fabric notebook definitions (notebook-content.py) from the plain sources in notebooks/src.

Source format: cells separated by ``# %%``; ``# %% [parameters]`` marks Fabric's parameters cell.
Output: notebooks/<name>/notebook-content.py, which Terraform deploys with GoTemplate tokens filled in
(lakehouse and workspace ids, Eventhouse URI).

    python scripts/build_notebooks.py          # write
    python scripts/build_notebooks.py --check  # fail if outputs are stale (CI)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "notebooks" / "src"
OUT = ROOT / "notebooks"


def meta(obj: dict) -> str:
    return "\n".join("# META " + line if line else "# META" for line in json.dumps(obj, indent=2).splitlines())


HEADER = {
    "kernel_info": {"name": "synapse_pyspark"},
    "dependencies": {
        "lakehouse": {
            "default_lakehouse": "{{ .LakehouseId }}",
            "default_lakehouse_name": "{{ .LakehouseName }}",
            "default_lakehouse_workspace_id": "{{ .WorkspaceId }}",
        }
    },
}
CELL_META = {"language": "python", "language_group": "synapse_pyspark"}


def split_cells(text: str) -> list[tuple[str, str]]:
    cells, kind, buf = [], "code", []
    for line in text.splitlines():
        if line.startswith("# %%"):
            if any(x.strip() for x in buf):
                cells.append((kind, "\n".join(buf).strip("\n")))
            kind = "parameters" if "[parameters]" in line else "code"
            buf = []
        else:
            buf.append(line)
    if any(x.strip() for x in buf):
        cells.append((kind, "\n".join(buf).strip("\n")))
    return cells


def build(name: str, setup: str) -> str:
    cells = split_cells((SRC / f"{name}.py").read_text())
    params = [c for c in cells if c[0] == "parameters"]
    code = [c for c in cells if c[0] == "code"]
    setup_body = "\n".join(line for line in setup.splitlines() if not line.startswith("# Shared first cell"))
    ordered = params + [("code", setup_body.strip())] + code
    parts = ["# Fabric notebook source", "", "# METADATA ********************", "", meta(HEADER)]
    for kind, body in ordered:
        marker = "# PARAMETERS CELL ********************" if kind == "parameters" else "# CELL ********************"
        parts += ["", marker, "", body, "", "# METADATA ********************", "", meta(CELL_META)]
    return "\n".join(parts) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    setup = (SRC / "_setup.py").read_text()
    stale = []
    for src in sorted(SRC.glob("nb_*.py")):
        content = build(src.stem, setup)
        target = OUT / src.stem / "notebook-content.py"
        if a.check:
            if not target.exists() or target.read_text() != content:
                stale.append(str(target.relative_to(ROOT)))
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
            print(f"built {target.relative_to(ROOT)}")
    if stale:
        sys.exit(f"stale notebook builds, run scripts/build_notebooks.py: {stale}")


if __name__ == "__main__":
    main()
