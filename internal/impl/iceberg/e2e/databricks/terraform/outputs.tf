output "catalog_name" {
  value = databricks_catalog.e2e.name
}

output "schema_name" {
  value = databricks_schema.e2e.name
}

output "warehouse_id" {
  value = databricks_sql_endpoint.e2e.id
}

output "workspace_host" {
  value = local.workspace_host
}

output "iceberg_rest_url" {
  value = "https://${local.workspace_host}/api/2.1/unity-catalog/iceberg-rest"
}
