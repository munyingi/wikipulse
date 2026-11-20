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

minutes = 55.0
key_vault_uri = "{{ .KeyVaultUri }}"  # Azure Key Vault holding the Eventstream connection string
secret_name = "eventstream-connection-string"

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

# Sends Wikimedia recent changes to the Eventstream custom endpoint for `minutes`, then exits.
# Scheduled hourly by Airflow, so the live feed keeps flowing without a server of your own.
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-eventhub>=5.11"])  # driver only

from wikipulse.recentchanges import run

conn = notebookutils.credentials.getSecret(key_vault_uri, secret_name)
sent = run(minutes, conn, log=lambda m: None)
print(f"sent {sent} events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
