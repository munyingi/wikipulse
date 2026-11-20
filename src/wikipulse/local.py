"""Run the batch pipeline locally on synthetic hours, without Fabric.

wikipulse-local --hours 6
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import ops, pipeline, sample
from .spark import local_spark


def main() -> None:
    ap = argparse.ArgumentParser(description="Run Wikipulse locally on synthetic page view files")
    ap.add_argument("--hours", type=int, default=6)
    ap.add_argument("--data", default="data")
    ap.add_argument("--skip", type=int, default=None, help="leave this hour index out, then backfill it")
    a = ap.parse_args()

    data = Path(a.data).resolve()
    start = datetime(2026, 11, 16, tzinfo=UTC)
    hours = [start + timedelta(hours=i) for i in range(a.hours)]
    for h in hours:
        sample.write_hour(data / "raw", h)

    spark = local_spark(warehouse=str(data / "warehouse"))
    for i, h in enumerate(hours):
        if i == a.skip:
            continue
        pipeline.process_hour(spark, str(data / "raw"), h, pipeline.expected_volume(spark))

    todo = ops.missing_hours(hours[0], hours[-1] + timedelta(hours=1), pipeline.loaded_hours(spark))
    print(f"backfill needed for {len(todo)} hour(s): {[f'{h:%Y-%m-%d %H:00}' for h in todo]}")
    for h in todo:
        pipeline.process_hour(spark, str(data / "raw"), h)
    spark.table(pipeline.T_TOP).orderBy("wiki", "rank").show(10, truncate=False)


if __name__ == "__main__":
    main()
