import gzip
import io
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest

from wikipulse.download import fetch_hour

H = datetime(2026, 11, 20, 13, tzinfo=UTC)


def gz(text: str) -> bytes:
    return gzip.compress(text.encode())


def opener_for(payload: bytes, calls: list):
    @contextmanager
    def _open(url):
        calls.append(url)
        yield io.BytesIO(payload)

    return _open


def test_download_is_idempotent(tmp_path):
    calls = []
    first = fetch_hour(H, str(tmp_path), opener=opener_for(gz("en A 1 0\nen B 2 0\n"), calls))
    second = fetch_hour(H, str(tmp_path), opener=opener_for(gz("x"), calls))
    assert first["lines"] == 2 and not first["skipped"]
    assert second["skipped"] and len(calls) == 1


def test_corrupt_download_leaves_no_file(tmp_path):
    with pytest.raises(OSError):
        fetch_hour(H, str(tmp_path), opener=opener_for(b"not gzip at all", []))
    assert not list(tmp_path.rglob("*.gz"))
