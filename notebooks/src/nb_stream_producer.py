# %% [parameters]
minutes = 55.0
key_vault_uri = "{{ .KeyVaultUri }}"  # Azure Key Vault holding the Eventstream connection string
secret_name = "eventstream-connection-string"

# %%
# Sends Wikimedia recent changes to the Eventstream custom endpoint for `minutes`, then exits.
# Scheduled hourly by Airflow, so the live feed keeps flowing without a server of your own.
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-eventhub>=5.11"])  # driver only

from wikipulse.recentchanges import run

conn = notebookutils.credentials.getSecret(key_vault_uri, secret_name)
sent = run(minutes, conn, log=lambda m: None)
print(f"sent {sent} events")
