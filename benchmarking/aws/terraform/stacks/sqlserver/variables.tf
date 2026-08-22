variable "region" {
  type    = string
  default = "us-east-2"
}
# Required (no default), same as the postgres/mysql stacks: the scenario's
# infra.source block must supply these three, and translateInfraSource turns
# each infra.source key into a -var.
variable "instance_class" { type = string }
variable "storage_gb" { type = number }
variable "iops" { type = number }

variable "engine" {
  type    = string
  default = "sqlserver-se"
}
variable "engine_version" {
  type    = string
  default = "15.00"
}
variable "parameter_group_family" {
  type    = string
  default = "sqlserver-se-15.0"
}
variable "parameters" {
  type    = map(string)
  default = {}
}
variable "storage_throughput" {
  # gp3 throughput MiB/s; null = RDS default. See modules/rds-mssql.
  type    = number
  default = null
}
