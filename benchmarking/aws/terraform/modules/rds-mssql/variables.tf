variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" {
  type = list(string)
}
variable "client_sg_ids" {
  type        = list(string)
  description = "SGs allowed to connect on 1433"
}
variable "instance_class" {
  # No Graviton for SQL Server on RDS — x86 (db.r5) only.
  type    = string
  default = "db.r5.2xlarge"
}
variable "storage_gb" {
  type    = number
  default = 400
}
variable "iops" {
  type    = number
  default = 12000
}
variable "engine" {
  type        = string
  description = "sqlserver-se (Standard) or sqlserver-ee (Enterprise). CDC is unsupported on sqlserver-ex/sqlserver-web."
  default     = "sqlserver-se"
}
variable "engine_version" {
  # Major-version prefix: RDS resolves the latest available minor at create
  # time, so this doesn't rot. Pin a full version (e.g. 15.00.4430.1.v1) if a
  # run needs to be reproducible — list them with:
  #   aws rds describe-db-engine-versions --engine sqlserver-se --engine-version 15.00
  type    = string
  default = "15.00"
}
variable "parameter_group_family" {
  # Must track the major version chosen above: sqlserver-se-15.0 for 2019,
  # sqlserver-se-16.0 for 2022.
  type    = string
  default = "sqlserver-se-15.0"
}
variable "db_name" {
  # NOT passed to aws_db_instance (RDS rejects db_name for SQL Server) — this
  # is the database the seeder CREATEs and the one both DSN outputs point at.
  type    = string
  default = "benchdb"
}
variable "master_username" {
  type    = string
  default = "bench"
}
variable "parameters" {
  # SQL Server CDC needs no parameter-group settings; see main.tf.
  type    = map(string)
  default = {}
}
variable "storage_throughput" {
  # gp3 throughput in MiB/s. null = RDS default, which the 2026-08-10..11 sweep
  # proved insufficient: WriteThroughput pinned at ~195-197 MB/s with
  # DiskQueueDepth to 249 while CPU sat under 50%, so the whole bench was
  # storage-bound. 24000 IOPS permits up to 1000 MiB/s (0.25 MiB/s per IOPS).
  type    = number
  default = null
}
