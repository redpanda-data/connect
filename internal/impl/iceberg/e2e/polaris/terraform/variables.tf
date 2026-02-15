variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "prefix" {
  description = "Resource name prefix"
  type        = string
  default     = "rpcntest"
}

variable "sp_client_id" {
  description = "Service principal client ID for Polaris ADLS access"
  type        = string
  default     = ""
}

variable "sp_client_secret" {
  description = "Service principal client secret for Polaris ADLS access"
  type        = string
  default     = ""
  sensitive   = true
}
