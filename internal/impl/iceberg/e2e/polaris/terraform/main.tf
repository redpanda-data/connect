terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
  required_version = ">= 1.0"
}

provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

# --- Resource Group ---

resource "azurerm_resource_group" "iceberg" {
  name     = "${var.prefix}-iceberg-e2e"
  location = var.location
}

# --- ADLS Gen2 Storage ---

resource "azurerm_storage_account" "iceberg" {
  name                     = "${var.prefix}iceberge2e"
  resource_group_name      = azurerm_resource_group.iceberg.name
  location                 = azurerm_resource_group.iceberg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_container" "warehouse" {
  name               = "warehouse"
  storage_account_id = azurerm_storage_account.iceberg.id
}

# --- Service Principal for Polaris ---
#
# Polaris needs Azure AD credentials to write table metadata to ADLS Gen2.
# Create one before running terraform apply:
#
#   az ad sp create-for-rbac --name "${prefix}-iceberg-e2e-polaris" \
#     --role "Storage Blob Data Contributor" \
#     --scopes "$(terraform output -raw storage_account_id)" \
#     --create-cert
#
#   az ad app credential reset --id <appId> --append --display-name "polaris-e2e"
#
# Then set the variables: sp_client_id, sp_client_secret

# --- Rendered example config ---

resource "local_file" "example_config" {
  filename = "${path.module}/example-config.yaml"
  content = templatefile("${path.module}/templates/example-config.yaml.tftpl", {
    storage_account_name = azurerm_storage_account.iceberg.name
    storage_access_key   = azurerm_storage_account.iceberg.primary_access_key
    container_name       = azurerm_storage_container.warehouse.name
  })
}
