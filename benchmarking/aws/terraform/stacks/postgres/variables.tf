variable "region" {
  type    = string
  default = "us-east-2"
}
variable "instance_class" { type = string }
variable "storage_gb" { type = number }
variable "iops" { type = number }
variable "parameters" {
  type    = map(string)
  default = { "rds.logical_replication" = "1", max_wal_senders = "20" }
}
variable "storage_throughput" {
  # gp3 throughput MiB/s; null = RDS default. See modules/rds-postgres.
  type    = number
  default = null
}
variable "iam_auth_enabled" {
  # RDS IAM database auth; set by soak scenarios via infra.source. See
  # modules/rds-postgres for what it exercises and why.
  type    = bool
  default = false
}
