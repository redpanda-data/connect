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
  description = "Managed storage root for the e2e catalog (e.g. s3://bucket/prefix). REQUIRED when the metastore has no default storage root — check with `databricks metastores summary`. Empty means inherit the metastore root (or, with create_storage = true, use the provisioned bucket). An explicit value always wins over create_storage."
  type        = string
  default     = ""
}

variable "create_storage" {
  description = "Provision catalog storage too: an S3 bucket + IAM role on the AWS side, and a Unity Catalog storage credential + external location on the Databricks side, used as the e2e catalog's storage_root. Needed on express-trial workspaces (default storage can't serve external Iceberg REST clients) and on metastores with no storage_root. Requires aws_region and AWS credentials in the environment."
  type        = bool
  default     = false

  validation {
    condition     = !var.create_storage || var.aws_region != ""
    error_message = "aws_region must be set when create_storage = true."
  }
}

variable "aws_region" {
  description = "AWS region for the provisioned catalog storage bucket (e.g. us-east-1). Only used — and required — when create_storage = true."
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
