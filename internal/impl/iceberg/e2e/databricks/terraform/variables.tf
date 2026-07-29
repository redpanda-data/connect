variable "prefix" {
  description = "Resource name prefix"
  type        = string
  default     = "rpcn"
}

variable "workspace_host" {
  description = "Databricks workspace host, e.g. dbc-abc123.cloud.databricks.com (scheme optional). Usually: export TF_VAR_workspace_host=\"$DATABRICKS_HOST\""
  type        = string
}

variable "schema_name" {
  description = "Unity Catalog schema (Iceberg namespace) to pre-create for the tests"
  type        = string
  default     = "e2e"
}

variable "storage_root" {
  description = "Managed storage root for the e2e catalog (e.g. s3://bucket/prefix). REQUIRED when the metastore has no default storage root — check with `databricks metastores summary`. Empty means inherit the metastore root."
  type        = string
  default     = ""
}

variable "manage_external_access" {
  description = "Have terraform enable external_access_enabled on the metastore via the Databricks CLI. Needs METASTORE ADMIN; usually a one-time manual action instead — see main.tf."
  type        = bool
  default     = false
}

variable "metastore_id" {
  description = "Metastore ID, only used when manage_external_access = true (find it with `databricks metastores summary`)"
  type        = string
  default     = ""
}
