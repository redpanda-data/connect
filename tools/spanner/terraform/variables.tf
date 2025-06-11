variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "sandbox-rpcn-457914"
}

variable "region" {
  description = "The GCP region for the Spanner instance"
  type        = string
  default     = "europe-central2"
}

variable "instance_name" {
  description = "Name of the Spanner instance"
  type        = string
  default     = "rpcn-tests-spanner"
}

variable "instance_config" {
  description = "The configuration for the Spanner instance"
  type        = string
  default     = "regional-europe-central2"
}

variable "instance_display_name" {
  description = "Display name for the Spanner instance"
  type        = string
  default     = "RedPanda Big Box Tests Spanner"
}

variable "instance_nodes" {
  description = "Number of nodes for the Spanner instance"
  type        = number
  default     = 1
}

variable "environment" {
  description = "Environment label for resources"
  type        = string
  default     = "dev"
}

variable "database_name" {
  description = "Name of the Spanner database"
  type        = string
  default     = "rpcn-tests"
}