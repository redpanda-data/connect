variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" {
  type = list(string)
}
variable "client_sg_ids" {
  type        = list(string)
  description = "SGs allowed to connect on 5432"
}
variable "instance_class" {
  type    = string
  default = "db.r6g.2xlarge"
}
variable "storage_gb" {
  type    = number
  default = 400
}
variable "iops" {
  type    = number
  default = 12000
}
variable "engine_version" {
  type    = string
  default = "16.14"
}
variable "db_name" {
  type    = string
  default = "benchdb"
}
variable "master_username" {
  type    = string
  default = "bench"
}
variable "parameters" {
  type = map(string)
  # rds.logical_replication=1 is the RDS-specific knob that makes RDS set
  # wal_level=logical for us (wal_level itself isn't user-settable on RDS).
  default = { "rds.logical_replication" = "1", max_wal_senders = "20" }
}
variable "storage_throughput" {
  # gp3 throughput in MiB/s. null = RDS default. The sqlserver 2026-08-10..11
  # sweep proved the default insufficient under CDC write amplification
  # (WriteThroughput pinned, DiskQueueDepth to 249, CPU <= 50%); the mysql
  # 8-vCPU degradation was suspected storage throttling with the same shape.
  # 24000 IOPS permits up to 1000 MiB/s (0.25 MiB/s per IOPS).
  type    = number
  default = null
}
variable "iam_auth_enabled" {
  # Enable RDS IAM database authentication. Soak scenarios turn this on so
  # the connector runs against ~15-minute IAM tokens (the rotation window
  # behind #4668/#4258); the DB-side role setup happens in the scenario's
  # reset steps, not here.
  type    = bool
  default = false
}
variable "iam_username" {
  # DB role the connector authenticates as when iam_auth_enabled. Created and
  # granted by the scenario reset (needs rds_iam + rds_replication); baked
  # into postgres_iam_dsn and the stack's rds-db:connect policy resource ARN.
  type    = string
  default = "bench_iam"
}
