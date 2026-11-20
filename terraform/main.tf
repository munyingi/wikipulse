locals {
  name          = "${var.name_prefix}-${var.environment}"
  notebook_dirs = toset([for f in fileset("${path.module}/../notebooks", "nb_*/notebook-content.py") : dirname(f)])
  kql_db_name   = "kql_wikipulse"
}

# ---------------------------------------------------------------- workspace and access
resource "fabric_workspace" "this" {
  display_name                   = local.name
  description                    = "Wikipulse ${var.environment}: real-time and batch analytics of Wikipedia activity"
  capacity_id                    = var.capacity_id
  skip_capacity_state_validation = true # trial capacities are not visible to every caller
}

resource "fabric_workspace_role_assignment" "extra" {
  for_each     = var.workspace_roles
  workspace_id = fabric_workspace.this.id
  principal = {
    id   = each.value.id
    type = each.value.type
  }
  role = each.value.role
}

# ---------------------------------------------------------------- storage
resource "fabric_lakehouse" "main" {
  display_name = "lh_wikipulse"
  description  = "Medallion lakehouse: bronze, silver, gold and ops schemas"
  workspace_id = fabric_workspace.this.id
  configuration = {
    enable_schemas = true
  }
}

resource "fabric_warehouse" "main" {
  display_name = "wh_wikipulse"
  description  = "Serving warehouse built by dbt, with row-level security and masking"
  workspace_id = fabric_workspace.this.id
}

# ---------------------------------------------------------------- real-time
resource "fabric_eventhouse" "main" {
  display_name = "eh_wikipulse"
  workspace_id = fabric_workspace.this.id
}

resource "fabric_kql_database" "main" {
  display_name = local.kql_db_name
  workspace_id = fabric_workspace.this.id
  format       = "Default"
  definition = {
    "DatabaseProperties.json" = {
      source = "${path.module}/templates/DatabaseProperties.json.tmpl"
      tokens = { EventhouseID = fabric_eventhouse.main.id }
    }
    "DatabaseSchema.kql" = {
      source          = "${path.module}/../kql/DatabaseSchema.kql"
      processing_mode = "None"
    }
  }
}

resource "fabric_eventstream" "recent_changes" {
  display_name = "es_recent_changes"
  description  = "Wikimedia recent changes: custom endpoint -> Eventhouse table RecentChanges"
  workspace_id = fabric_workspace.this.id
  format       = "Default"
  definition = {
    "eventstream.json" = {
      source = "${path.module}/templates/eventstream.json.tmpl"
      tokens = {
        WorkspaceID   = fabric_workspace.this.id
        KqlDatabaseID = fabric_kql_database.main.id
        DatabaseName  = local.kql_db_name
      }
    }
  }
}

# ---------------------------------------------------------------- notebooks
resource "fabric_notebook" "nb" {
  for_each     = local.notebook_dirs
  display_name = each.key
  workspace_id = fabric_workspace.this.id
  format       = "py"
  definition = {
    "notebook-content.py" = {
      source = "${path.module}/../notebooks/${each.key}/notebook-content.py"
      tokens = {
        WorkspaceId   = fabric_workspace.this.id
        LakehouseId   = fabric_lakehouse.main.id
        LakehouseName = fabric_lakehouse.main.display_name
        KustoQueryUri = fabric_eventhouse.main.properties.query_service_uri
        KustoDatabase = local.kql_db_name
        KeyVaultUri   = var.key_vault_uri
      }
    }
  }
}

# ---------------------------------------------------------------- orchestration
resource "fabric_apache_airflow_job" "orchestrator" {
  count        = var.enable_airflow_job ? 1 : 0
  display_name = "af_wikipulse"
  description  = "Hourly page views, daily edits and maintenance, freshness checks"
  workspace_id = fabric_workspace.this.id
  definition = {
    "apacheairflowjob-content.json" = {
      source = "${path.module}/templates/apacheairflowjob-content.json.tmpl"
      tokens = {
        AirflowVersion = var.airflow_version
        Environment    = jsonencode(local.airflow_env)
      }
    }
  }
}

locals {
  # The DAGs read item IDs from environment variables, so the same DAG code runs in dev and prod.
  airflow_env = merge(
    {
      WIKIPULSE_ENV          = var.environment
      WIKIPULSE_WORKSPACE_ID = fabric_workspace.this.id
      WIKIPULSE_ALERT_URL    = var.alert_webhook_url
    },
    { for k, nb in fabric_notebook.nb : "WIKIPULSE_${upper(k)}_ID" => nb.id }
  )
}
