"""Synthetic page view files in the exact Wikimedia dump format, for tests and offline demos.

Traffic follows a Zipf-like distribution with a few very hot pages, which is what makes skew handling matter.
"""

from __future__ import annotations

import gzip
import random
from datetime import datetime
from pathlib import Path

from .pageviews import hour_floor, raw_path

DOMAINS = [
    "en",
    "en.m",
    "de",
    "de.m",
    "fr",
    "fr.m",
    "es.m",
    "fi",
    "fi.m",
    "sw",
    "sw.m",
    "en.d",
    "de.voy",
    "commons.m",
    "commons.m.m",
    "www.wd",
    "en.b",
]
WEIGHTS = [30, 45, 6, 9, 4, 6, 8, 1, 2, 0.3, 0.6, 1, 0.3, 2, 1, 1.5, 0.2]
HOT = ["Main_Page", "Special:Search", "-"]


def lines_for_hour(hour: datetime, n_pages: int = 3000, seed: int = 42) -> list[str]:
    rng = random.Random(f"{seed}-{hour_floor(hour):%Y%m%d%H}")
    out = []
    for i in range(n_pages):
        dom = rng.choices(DOMAINS, weights=WEIGHTS)[0]
        title = HOT[i % len(HOT)] if i < 6 else f"Article_{int(rng.paretovariate(1.2) * 10) % 5000}"
        if rng.random() < 0.03:
            title = f"Talk:{title}"
        views = max(1, int(rng.paretovariate(1.1) * (5000 if title in HOT else 3)))
        out.append(f"{dom} {title} {views} 0")
    out.append("en broken_line_without_counts")
    out.append("de Some_Page not_a_number 0")
    return out


def write_hour(root: str | Path, hour: datetime, **kw) -> Path:
    path = Path(raw_path(str(root), hour))
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(lines_for_hour(hour, **kw)) + "\n")
    return path
