"""Idempotent writes to Delta tables.

* Hourly silver data replaces exactly one hour (``replaceWhere``), so re-running an hour or backfilling it
  can never double count.
* Gold daily tables are upserted with MERGE on their business key for the days that changed.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def table_exists(spark: SparkSession, table: str) -> bool:
    return spark.catalog.tableExists(table)


def replace_partition(df: DataFrame, table: str, predicate: str, partition_by: list[str] | None = None) -> None:
    """Overwrite only the rows matching ``predicate`` (for example ``hour_utc = '2026-11-20 13:00:00'``)."""
    spark = df.sparkSession
    if not table_exists(spark, table):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table)
        return
    df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(table)


def merge_upsert(df: DataFrame, table: str, keys: list[str], partition_by: list[str] | None = None) -> None:
    """Insert new keys and update changed rows. Rows not present in ``df`` are left alone."""
    spark = df.sparkSession
    if not table_exists(spark, table):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table)
        return
    from delta.tables import DeltaTable

    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def maintain(spark: SparkSession, table: str, zorder_by: list[str] | None = None, retain_hours: int = 168) -> None:
    """Compact small files and remove files no longer referenced. Runs daily, outside the hourly path."""
    zorder = f" ZORDER BY ({', '.join(zorder_by)})" if zorder_by else ""
    spark.sql(f"OPTIMIZE {table}{zorder}")
    spark.sql(f"VACUUM {table} RETAIN {retain_hours} HOURS")
