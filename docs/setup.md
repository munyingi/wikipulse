# Setup on Microsoft Fabric (free trial)

About 30 minutes. You need a work or school (Microsoft Entra ID) account; personal Microsoft accounts cannot start a Fabric trial.

## 1. Start the trial and enable features

1. Go to https://app.fabric.microsoft.com, open **Account manager > Free trial** and start the 60-day trial.
2. **Admin portal > Tenant settings** (you are tenant admin of a trial tenant you created; otherwise ask your admin):
   * *Users can create Fabric items*: on
   * *Users can create and use Apache Airflow jobs*: on
   * *Service principals can use Fabric APIs*: on (only for CI deploys)
3. **Admin portal > Capacity settings > Trial**: copy the capacity ID.

## 2. Deploy the infrastructure

```bash
az login                                   # your user; the provider uses the Azure CLI token
export FABRIC_USE_CLI=true
export TF_VAR_capacity_id=<capacity id>
cd terraform
terraform init
terraform workspace new dev
terraform apply -var-file=envs/dev.tfvars
```

This creates the `wikipulse-dev` workspace with the lakehouse, warehouse, Eventhouse and KQL database (schema included), Eventstream, the seven notebooks and the Airflow job.

## 3. Upload the package and the DAGs

```bash
cd ..
pip install -e ".[deploy]"
python scripts/deploy.py --env dev
```

## 4. Connect Airflow to Fabric

The DAGs call the Fabric job API through the Airflow connection `fabric_wikipulse`.

1. In Entra ID, register an app (or reuse one), create a client secret, and give the app **Contributor** on the `wikipulse-dev` workspace (Manage access).
2. Open the Airflow job `af_wikipulse` > Airflow UI > Admin > Connections > add `fabric_wikipulse` of type *Generic / Fabric* with tenant ID, client ID and secret (see Microsoft's tutorial "Run a Fabric data pipeline and notebook using Apache Airflow DAG").
3. For the dbt task, add the environment variables `WIKIPULSE_WAREHOUSE_HOST` (terraform output `warehouse_connection_string`), `WIKIPULSE_TENANT_ID`, `WIKIPULSE_CLIENT_ID`, `WIKIPULSE_CLIENT_SECRET` in the Airflow job settings.
4. Unpause `wikipulse_pageviews_hourly`, `wikipulse_daily` and `wikipulse_freshness`.

## 5. Turn on the live feed (optional)

1. Open the Eventstream `es_recent_changes`, select the custom endpoint source and copy the **Event Hub connection string**.
2. Store it in an Azure Key Vault as `eventstream-connection-string`, grant your user (or the workspace identity) *Key Vault Secrets User*, then set `key_vault_uri` in `envs/dev.tfvars` and re-apply.
3. Unpause `wikipulse_stream_producer`. Or run it from your laptop instead:

   ```bash
   pip install -e ".[stream]"
   export EVENTSTREAM_CONNECTION_STRING="Endpoint=sb://..."
   wikipulse-stream --minutes 30
   ```

4. Create a Real-Time Dashboard on the KQL database and paste the tiles from [`kql/dashboard_queries.kql`](../kql/dashboard_queries.kql).

## 6. Row-level security users

Replace the example UPNs in [`dbt/seeds/analyst_language_access.csv`](../dbt/seeds/analyst_language_access.csv) with real users of your tenant (give yourself `*`), then let the daily DAG run or run `dbt build` locally:

```bash
cd dbt && export WIKIPULSE_WAREHOUSE_HOST=<warehouse host> && dbt build --profiles-dir .
```

## 7. Deploy from GitHub Actions (optional)

1. Create a storage account with a `tfstate` container for remote state.
2. Add a federated credential to the Entra app for `repo:munyingi/wikipulse:environment:dev` (and `prod`).
3. Repository secrets: `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CAPACITY_ID`, `TFSTATE_RESOURCE_GROUP`, `TFSTATE_STORAGE_ACCOUNT`, optional `ALERT_WEBHOOK_URL`.
4. Create GitHub environments `dev` and `prod`; require a reviewer on `prod`.
5. Run the **deploy** workflow.

## Tearing down

`terraform destroy -var-file=envs/dev.tfvars` removes the workspace and everything in it. When the trial ends, items stay in OneLake for 7 days and can be re-created on a new trial from this repo in minutes.
