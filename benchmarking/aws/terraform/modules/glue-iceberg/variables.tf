variable "name_prefix" {
  type        = string
  description = "Prefix for the warehouse bucket name (made globally unique with the account id)."
}

variable "region" {
  type        = string
  description = "AWS region; used to construct the Glue Iceberg REST endpoint."
}

variable "database_name" {
  type        = string
  default     = "bench"
  description = "Glue catalog database (Iceberg namespace) both engines write to. Must match sinkSpecs[\"iceberg\"].Namespace in the runner."
}
