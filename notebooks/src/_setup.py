# Shared first cell, prepended to every Spark notebook by scripts/build_notebooks.py
import sys

LIB = "/lakehouse/default/Files/libs/wikipulse.zip"  # uploaded by scripts/deploy.py
if LIB not in sys.path:
    sys.path.insert(0, LIB)

spark.conf.set("spark.sql.parquet.vorder.default", "true")  # V-Order: faster reads for SQL endpoint and Direct Lake
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.session.timeZone", "UTC")
RAW_ROOT = "/lakehouse/default/Files/raw"
