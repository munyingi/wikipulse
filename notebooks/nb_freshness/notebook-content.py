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

sla_hours = 3.0

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

from datetime import UTC, datetime

from wikipulse import ops, pipeline

loaded = pipeline.loaded_hours(spark)
f = ops.freshness(max(loaded) if loaded else None, datetime.now(UTC), sla_hours)
print(f"latest hour {f.latest_loaded}, lag {f.lag_hours} h, SLA {f.sla_hours} h")
if not f.ok:
    raise RuntimeError(f"Freshness SLA breached: page views are {f.lag_hours} h behind (SLA {f.sla_hours} h)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
