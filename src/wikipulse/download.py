"""Idempotent download of one hourly page view dump into OneLake Files (or any local path)."""

from __future__ import annotations

import gzip
import os
import shutil
from datetime import datetime
from pathlib import Path

from .pageviews import dump_url, raw_path

USER_AGENT = "wikipulse/1.0 (portfolio project; https://github.com/munyingi/wikipulse)"


class NotPublishedYet(RuntimeError):
    """The dump for this hour is not on the server yet. Airflow retries the task later."""


def _verify_gzip(path: Path) -> int:
    lines = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        for _ in fh:
            lines += 1
    if lines == 0:
        raise ValueError(f"{path} is empty")
    return lines


def fetch_hour(hour: datetime, root: str, opener=None, force: bool = False) -> dict:
    """Download the dump for ``hour`` to ``<root>/pageviews/YYYY/MM/DD/``. Skips files that already exist and
    pass the gzip check. Writes to a temporary name first, so a failed download never leaves a partial file."""
    target = Path(raw_path(root, hour))
    if target.exists() and not force:
        return {"path": str(target), "skipped": True, "lines": _verify_gzip(target)}

    url = dump_url(hour)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".part")
    if opener is None:
        import requests

        resp = requests.get(url, stream=True, timeout=120, headers={"User-Agent": USER_AGENT})
        if resp.status_code == 404:
            raise NotPublishedYet(url)
        resp.raise_for_status()
        with open(tmp, "wb") as out:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                out.write(chunk)
    else:
        with opener(url) as src, open(tmp, "wb") as out:
            shutil.copyfileobj(src, out)
    lines = _verify_gzip(tmp)
    os.replace(tmp, target)
    return {"path": str(target), "skipped": False, "lines": lines, "bytes": target.stat().st_size, "url": url}
