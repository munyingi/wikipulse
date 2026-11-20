"""Gold layer: business-level aggregates built from silver hourly page views."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def wiki_hourly_traffic(silver: DataFrame) -> DataFrame:
    """Views per wiki, access method and hour. Small table that drives most dashboards."""
    return silver.groupBy("hour_utc", "date_utc", "wiki", "project", "language", "access").agg(
        F.sum("views").alias("views"),
        F.countDistinct("page_title").alias("pages_viewed"),
    )


def daily_page_views(silver: DataFrame) -> DataFrame:
    """One row per day, wiki and page, with desktop and mobile split out."""
    return silver.groupBy("date_utc", "wiki", "project", "language", "page_title", "is_article").agg(
        F.sum(F.when(F.col("access") == "desktop", F.col("views")).otherwise(0)).alias("views_desktop"),
        F.sum(F.when(F.col("access") == "mobile", F.col("views")).otherwise(0)).alias("views_mobile"),
        F.sum("views").alias("views"),
        F.countDistinct("hour_utc").alias("hours_with_views"),
    )


def top_pages_daily(daily: DataFrame, n: int = 100) -> DataFrame:
    """Top ``n`` articles per wiki and day. Main pages are excluded, they top every list."""
    w = Window.partitionBy("date_utc", "wiki").orderBy(F.desc("views"), F.asc("page_title"))
    return (
        daily.where(F.col("is_article") & ~F.col("page_title").isin("Main_Page", "-"))
        .withColumn("rank", F.row_number().over(w))
        .where(F.col("rank") <= n)
    )


def trending_pages(daily: DataFrame, min_views: int = 1000, lookback_days: int = 7) -> DataFrame:
    """Spike score = today's views divided by the average of the previous ``lookback_days`` days.

    Pages with no history get the score of a page that had one view a day, so brand-new pages
    with a burst of traffic still surface.
    """
    w = (
        Window.partitionBy("wiki", "page_title")
        .orderBy(F.col("date_utc").cast("timestamp").cast("long"))
        .rangeBetween(-lookback_days * 86400, -1)
    )
    baseline = F.avg("views").over(w)
    return (
        daily.where("is_article")
        .withColumn("baseline_views", F.coalesce(baseline, F.lit(1.0)))
        .withColumn("spike_score", F.round(F.col("views") / F.greatest(F.col("baseline_views"), F.lit(1.0)), 2))
        .where(F.col("views") >= min_views)
    )


def salted_join(
    big: DataFrame, small: DataFrame, keys: list[str], hot_keys: list[tuple], buckets: int = 16, how: str = "left"
) -> DataFrame:
    """Join that spreads known hot keys over ``buckets`` partitions.

    A handful of pages (Main_Page, Special:Search) carry a large share of all traffic, so a plain shuffle join
    puts most of the work on one task. Rows of the big side with a hot key get a random salt; the matching
    rows of the small side are replicated once per salt value. Cold keys use salt 0 on both sides.
    """
    hot = (
        F.struct(*[F.col(k) for k in keys]).isin([F.struct(*[F.lit(v) for v in hk]) for hk in hot_keys])
        if hot_keys
        else F.lit(False)
    )
    big_s = big.withColumn("_salt", F.when(hot, (F.rand(seed=7) * buckets).cast("int")).otherwise(F.lit(0)))
    salts = F.when(hot, F.sequence(F.lit(0), F.lit(buckets - 1))).otherwise(F.array(F.lit(0)))
    small_s = small.withColumn("_salt", F.explode(salts))
    return big_s.join(small_s, on=[*keys, "_salt"], how=how).drop("_salt")


def edits_vs_views(daily_views: DataFrame, daily_edits: DataFrame, hot_keys: list[tuple] | None = None) -> DataFrame:
    """Daily views next to daily edits per page: which pages get attention, which get work."""
    keys = ["date_utc", "wiki", "page_title"]
    joined = salted_join(
        daily_views, daily_edits.select(*keys, "edits", "editors", "bytes_changed"), keys, hot_keys or []
    )
    return joined.select(
        *keys,
        "views",
        F.coalesce("edits", F.lit(0)).alias("edits"),
        F.coalesce("editors", F.lit(0)).alias("editors"),
        F.coalesce("bytes_changed", F.lit(0)).alias("bytes_changed"),
    )
