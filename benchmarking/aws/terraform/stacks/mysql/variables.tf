variable "region" {
  type    = string
  default = "us-east-2"
}
variable "bench_session_id" {
  # Runner-generated session ID ("bench-YYYYMMDD-HHMMSS", see newSessionID).
  # Stamped on every resource via default_tags; the cleanup lambda decodes
  # the embedded timestamp as the only age signal for resources whose
  # Describe response carries no creation time. Same contract as the shared
  # stack's variable of the same name, including the empty default: `runner
  # down` rebuilds stack vars from the scenario's infra.source only, so a
  # required variable would abort the destroy (-input=false) and strand paid
  # infra until the reaper's TTL. The tag value doesn't matter during destroy.
  type    = string
  default = ""
}
variable "instance_class" { type = string }
variable "storage_gb" { type = number }
variable "iops" { type = number }
variable "parameters" {
  type = map(string)
  # binlog_format=ROW + binlog_row_image=FULL are what mysql_cdc requires;
  # binlog_checksum=NONE keeps go-mysql compatible across server versions.
  default = {
    binlog_format    = "ROW"
    binlog_row_image = "FULL"
    binlog_checksum  = "NONE"
  }
}
variable "storage_throughput" {
  # gp3 throughput MiB/s; null = RDS default. See modules/rds-mysql.
  type    = number
  default = null
}
