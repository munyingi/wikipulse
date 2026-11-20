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

hour = ""  # ISO hour in UTC, e.g. 2026-11-20T13:00:00Z. Empty = latest published hour.

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

from wikipulse import ops
from wikipulse.download import fetch_hour

h = datetime.fromisoformat(hour.replace("Z", "+00:00")) if hour else ops.latest_available_hour(datetime.now(UTC))
result = fetch_hour(h, RAW_ROOT)
print(result)
notebookutils.notebook.exit(result["path"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
