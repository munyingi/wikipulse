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

from wikipulse import pipeline
from wikipulse.delta_io import maintain

for table, zorder in [
    (pipeline.T_BRONZE, None),
    (pipeline.T_SILVER, ["wiki"]),
    (pipeline.T_DAILY, ["wiki"]),
    (pipeline.T_HOURLY, None),
    (pipeline.T_TOP, None),
    (pipeline.T_TRENDING, None),
]:
    if spark.catalog.tableExists(table):
        maintain(spark, table, zorder_by=zorder)
        print(f"maintained {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Daily cost roll-up from the run log: estimated capacity units and euros per pipeline and day.
spark.sql(f"""
CREATE OR REPLACE TABLE ops.cost_daily AS
SELECT date(finished_at) AS day, pipeline, count(*) AS runs, sum(seconds) AS seconds,
       sum(cu_seconds) AS cu_seconds, round(sum(eur_estimate), 4) AS eur_estimate
FROM {pipeline.T_RUNS}
GROUP BY 1, 2
""")
display(spark.table("ops.cost_daily").orderBy("day", ascending=False).limit(30))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
