# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "{{ .LakehouseId }}",
# META       "default_lakehouse_name": "{{ .LakehouseName }}",
# META       "default_lakehouse_workspace_id": "{{ .WorkspaceId }}"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

start = ""  # ISO hour, inclusive. Empty = 48 hours ago.
end = ""    # ISO hour, exclusive. Empty = latest published hour + 1.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys

LIB = "/lakehouse/default/Files/libs/wikipulse.zip"  # uploaded by scripts/deploy.py
if LIB not in sys.path:
    sys.path.insert(0, LIB)

spark.conf.set("spark.sql.parquet.vorder.default", "true")  # V-Order: faster reads for SQL endpoint and Direct Lake
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.session.timeZone", "UTC")
RAW_ROOT = "/lakehouse/default/Files/raw"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime, timedelta

from wikipulse import ops, pipeline
from wikipulse.download import NotPublishedYet, fetch_hour

now = datetime.now(UTC)
s = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else now - timedelta(hours=48)
e = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else ops.latest_available_hour(now) + timedelta(hours=1)

todo = ops.missing_hours(s, e, pipeline.loaded_hours(spark))
print(f"{len(todo)} hour(s) to backfill")
done = []
for h in todo:
    try:
        fetch_hour(h, RAW_ROOT)
    except NotPublishedYet:
        print(f"{h:%Y-%m-%d %H:00} not published yet, stopping")
        break
    pipeline.process_hour(spark, RAW_ROOT, h, expected_rows=pipeline.expected_volume(spark))
    done.append(f"{h:%Y-%m-%dT%H:00Z}")
notebookutils.notebook.exit(",".join(done))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
