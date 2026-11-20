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

day = ""  # YYYY-MM-DD. Empty = yesterday (UTC).
kusto_uri = "{{ .KustoQueryUri }}"
kusto_database = "{{ .KustoDatabase }}"

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

from datetime import UTC, date, datetime, timedelta

from pyspark.sql import functions as F

from wikipulse import pipeline
from wikipulse.delta_io import replace_partition

d = date.fromisoformat(day) if day else (datetime.now(UTC) - timedelta(days=1)).date()
# One query to the Eventhouse, aggregated there: only the daily summary crosses into Spark.
query = f"""
materialized_view('RecentChangesDedup')
| where event_time >= datetime({d}) and event_time < datetime({d + timedelta(days=1)})
| where change_type in ('edit', 'new') and namespace == 0
| extend wiki = strcat(tostring(split(server_name, '.')[0]), '.', tostring(split(server_name, '.')[1]))
| summarize edits = count(), editors = dcount(editor), bytes_changed = sum(abs(bytes_changed))
    by wiki, page_title = replace_string(title, ' ', '_')
"""
edits = (
    spark.read.format("com.microsoft.kusto.spark.synapse.datasource")
    .option("kustoCluster", kusto_uri)
    .option("kustoDatabase", kusto_database)
    .option("kustoQuery", query)
    .option("accessToken", notebookutils.credentials.getToken(kusto_uri))
    .load()
    .withColumn("date_utc", F.lit(d))
)
pipeline.ensure_schemas(spark)
replace_partition(edits, pipeline.T_EDITS, f"date_utc = DATE '{d}'", partition_by=["date_utc"])
pipeline.join_edits(spark, d)
print(f"{edits.count():,} edited pages on {d}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Edits per editor and day. Editor names are personal data: the warehouse masks them (see dbt model).
editor_query = f"""
materialized_view('RecentChangesDedup')
| where event_time >= datetime({d}) and event_time < datetime({d + timedelta(days=1)})
| where change_type in ('edit', 'new')
| extend wiki = strcat(tostring(split(server_name, '.')[0]), '.', tostring(split(server_name, '.')[1]))
| summarize edits = count(), pages = dcount(title), bytes_changed = sum(abs(bytes_changed))
    by wiki, editor, is_bot
"""
editors = (
    spark.read.format("com.microsoft.kusto.spark.synapse.datasource")
    .option("kustoCluster", kusto_uri)
    .option("kustoDatabase", kusto_database)
    .option("kustoQuery", editor_query)
    .option("accessToken", notebookutils.credentials.getToken(kusto_uri))
    .load()
    .withColumn("date_utc", F.lit(d))
)
replace_partition(editors, "gold.editors_daily", f"date_utc = DATE '{d}'", partition_by=["date_utc"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
