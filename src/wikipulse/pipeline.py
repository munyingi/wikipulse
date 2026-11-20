"""The hourly batch pipeline, shared by the Fabric notebooks and the local runner.

    raw file (OneLake Files)  ->  bronze.pageviews_raw  ->  silver.page_views_hourly
                                                         ->  gold.wiki_hourly_traffic
                                                         ->  gold.daily_page_views -> gold.top_pages_daily
                                                                                   -> gold.trending_pages
Every write replaces exactly the hour or day it covers, so any hour can be re-run or backfilled safely.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import gold, ops, pageviews, quality
from .delta_io import merge_upsert, replace_partition

SCHEMAS = ("bronze", "silver", "gold", "ops")
T_BRONZE = "bronze.pageviews_raw"
T_SILVER = "silver.page_views_hourly"
T_HOURLY = "gold.wiki_hourly_traffic"
T_DAILY = "gold.daily_page_views"
T_TOP = "gold.top_pages_daily"
T_TRENDING = "gold.trending_pages"
T_EDITS = "gold.edits_daily"
T_EDITS_VS_VIEWS = "gold.edits_vs_views"
T_STATUS = "ops.hour_status"
T_QUALITY = "ops.quality_results"
T_RUNS = "ops.pipeline_runs"

HOT_KEYS_SQL = "Main_Page|Special:Search"


def ensure_schemas(spark: SparkSession) -> None:
    for s in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")


def _ts(hour: datetime) -> str:
    return f"{pageviews.hour_floor(hour):%Y-%m-%d %H:%M:%S}"


def bronze_hour(spark: SparkSession, raw_root: str, hour: datetime) -> DataFrame:
    path = pageviews.raw_path(raw_root, hour)
    lines = spark.read.text(path)
    parsed = pageviews.parse_lines(lines, hour)
    # keep the raw line only where parsing failed, to save space but allow investigation
    bad = (F.col("n_fields") != 4) | F.col("views").isNull()
    return parsed.withColumn("raw_line", F.when(bad, F.col("raw_line"))).withColumn("date_utc", F.to_date("hour_utc"))


def process_hour(
    spark: SparkSession, raw_root: str, hour: datetime, expected_rows: float = 0, vcores: float = 8, log=print
) -> dict:
    """Load one hour end to end. Returns a summary that is also stored in ``ops``."""
    started = time.monotonic()
    run_id = str(uuid.uuid4())
    h = pageviews.hour_floor(hour)
    hour_pred = f"hour_utc = TIMESTAMP '{_ts(h)}'"
    day = h.date()
    ensure_schemas(spark)

    bronze = bronze_hour(spark, raw_root, h).cache()
    results = [
        quality.row_count_at_least(bronze, T_BRONZE, 1),
        quality.malformed_ratio_at_most(bronze, T_BRONZE, 0.01),
    ]
    replace_partition(bronze, T_BRONZE, hour_pred, partition_by=["date_utc"])

    silver = pageviews.to_silver(bronze).cache()
    n_silver = silver.count()
    results += [
        quality.not_null(silver, T_SILVER, ["hour_utc", "wiki", "page_title", "views"]),
        quality.unique(silver, T_SILVER, ["hour_utc", "wiki", "access", "page_title"]),
        quality.volume_within(n_silver, expected_rows, T_SILVER),
    ]
    _write_quality(spark, results, h, run_id)
    quality.enforce(results)  # stop before gold if an error-level check failed
    replace_partition(silver, T_SILVER, hour_pred, partition_by=["date_utc"])

    replace_partition(gold.wiki_hourly_traffic(silver), T_HOURLY, hour_pred, partition_by=["date_utc"])
    rebuild_day(spark, day)

    seconds = time.monotonic() - started
    cu = ops.cu_seconds(vcores, seconds)
    summary = {
        "run_id": run_id,
        "pipeline": "pageviews_hourly",
        "hour_utc": h.replace(tzinfo=None),
        "status": "succeeded",
        "rows_bronze": bronze.count(),
        "rows_silver": n_silver,
        "seconds": round(seconds, 1),
        "cu_seconds": round(cu, 1),
        "eur_estimate": ops.eur_estimate(cu),
        "finished_at": datetime.now(UTC).replace(tzinfo=None),
    }
    _record_run(spark, summary)
    bronze.unpersist()
    silver.unpersist()
    log(f"{_ts(h)}: {n_silver:,} silver rows in {seconds:.0f}s")
    return summary


def rebuild_day(spark: SparkSession, day) -> None:
    """Recompute the daily gold tables for one day from silver. Cheap, and correct after late or re-run hours."""
    day_pred = f"date_utc = DATE '{day}'"
    silver_day = spark.table(T_SILVER).where(day_pred)
    daily = gold.daily_page_views(silver_day).cache()
    replace_partition(daily, T_DAILY, day_pred, partition_by=["date_utc"])
    replace_partition(gold.top_pages_daily(daily), T_TOP, day_pred, partition_by=["date_utc"])

    lookback_start = day - timedelta(days=7)
    history = spark.table(T_DAILY).where(f"date_utc BETWEEN DATE '{lookback_start}' AND DATE '{day}'")
    trending = gold.trending_pages(history).where(day_pred)
    replace_partition(trending, T_TRENDING, day_pred, partition_by=["date_utc"])
    daily.unpersist()


def join_edits(spark: SparkSession, day) -> None:
    """Join daily views with daily edits (exported from the Eventhouse). Hot pages are salted."""
    if not spark.catalog.tableExists(T_EDITS):
        return
    day_pred = f"date_utc = DATE '{day}'"
    views = spark.table(T_DAILY).where(day_pred)
    edits = spark.table(T_EDITS).where(day_pred)
    hot = [
        (r.date_utc, r.wiki, r.page_title)
        for r in views.where(F.col("page_title").rlike(f"^({HOT_KEYS_SQL})$"))
        .select("date_utc", "wiki", "page_title")
        .collect()
    ]
    replace_partition(gold.edits_vs_views(views, edits, hot), T_EDITS_VS_VIEWS, day_pred, partition_by=["date_utc"])


def _write_quality(spark: SparkSession, results: list, hour: datetime, run_id: str) -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        {
            **r.as_row(),
            "observed": float(r.observed),
            "threshold": float(r.threshold),
            "hour_utc": hour.replace(tzinfo=None),
            "run_id": run_id,
            "checked_at": now,
        }
        for r in results
    ]
    merge_upsert(spark.createDataFrame(rows), T_QUALITY, ["hour_utc", "check", "table"])


def _record_run(spark: SparkSession, summary: dict) -> None:
    run = spark.createDataFrame([summary])
    merge_upsert(run, T_RUNS, ["run_id"])
    status = run.select("hour_utc", "status", "rows_silver", F.col("finished_at").alias("loaded_at"))
    merge_upsert(status, T_STATUS, ["hour_utc"])


def loaded_hours(spark: SparkSession) -> set[datetime]:
    if not spark.catalog.tableExists(T_STATUS):
        return set()
    return {
        r.hour_utc.replace(tzinfo=UTC)
        for r in spark.table(T_STATUS).where("status = 'succeeded'").select("hour_utc").collect()
    }


def expected_volume(spark: SparkSession, hours: int = 24) -> float:
    """Average silver rows per hour over the last loaded hours, for the volume check."""
    if not spark.catalog.tableExists(T_STATUS):
        return 0.0
    row = spark.sql(
        f"SELECT avg(rows_silver) AS a FROM (SELECT rows_silver FROM {T_STATUS} "
        f"WHERE status = 'succeeded' ORDER BY hour_utc DESC LIMIT {hours})"
    ).first()
    return float(row.a or 0)
