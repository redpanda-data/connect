output "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.iceberg.name
}

output "storage_account_id" {
  description = "ADLS Gen2 storage account resource ID (for role assignments)"
  value       = azurerm_storage_account.iceberg.id
}

output "storage_access_key" {
  description = "ADLS Gen2 storage account access key"
  value       = azurerm_storage_account.iceberg.primary_access_key
  sensitive   = true
}

output "container_name" {
  description = "ADLS Gen2 container name"
  value       = azurerm_storage_container.warehouse.name
}

output "location" {
  description = "Azure region"
  value       = var.location
}

output "tenant_id" {
  description = "Azure tenant ID"
  value       = data.azurerm_client_config.current.tenant_id
}

output "sp_client_id" {
  description = "Service principal client ID for Polaris"
  value       = var.sp_client_id
}

output "sp_client_secret" {
  description = "Service principal client secret for Polaris"
  value       = var.sp_client_secret
  sensitive   = true
}
