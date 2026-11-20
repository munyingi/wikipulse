output "workspace_id" {
  value = fabric_workspace.this.id
}

output "lakehouse_id" {
  value = fabric_lakehouse.main.id
}

output "lakehouse_sql_endpoint" {
  description = "Use as the host for dbt and SQL clients."
  value       = fabric_lakehouse.main.properties.sql_endpoint_properties.connection_string
}

output "warehouse_connection_string" {
  value = fabric_warehouse.main.properties.connection_string
}

output "eventhouse_query_uri" {
  value = fabric_eventhouse.main.properties.query_service_uri
}

output "notebook_ids" {
  value = { for k, nb in fabric_notebook.nb : k => nb.id }
}

output "eventstream_id" {
  value = fabric_eventstream.recent_changes.id
}
