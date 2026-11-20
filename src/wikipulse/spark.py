"""Local Spark session with Delta Lake, for tests and the offline runner. Fabric notebooks use their own session."""

from __future__ import annotations

import os
import tempfile


def delta_available() -> bool:
    try:
        import delta  # noqa: F401
    except ImportError:
        return False
    return os.environ.get("WIKIPULSE_NO_DELTA") != "1"


def local_spark(app: str = "wikipulse", warehouse: str | None = None):
    from pyspark.sql import SparkSession

    warehouse = warehouse or tempfile.mkdtemp(prefix="wikipulse-warehouse-")
    builder = (
        SparkSession.builder.appName(app)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
    )
    if delta_available():
        from delta import configure_spark_with_delta_pip

        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()
