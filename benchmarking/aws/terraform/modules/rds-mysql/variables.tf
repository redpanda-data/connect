variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" {
  type = list(string)
}
variable "client_sg_ids" {
  type        = list(string)
  description = "SGs allowed to connect on 3306"
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
  default = "8.0.46"
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
  # binlog_format=ROW + binlog_row_image=FULL are required by mysql_cdc.
  # binlog_checksum=NONE keeps the go-mysql client compatible across versions.
  default = {
    binlog_format    = "ROW"
    binlog_row_image = "FULL"
    binlog_checksum  = "NONE"
  }
}
variable "storage_throughput" {
  # gp3 throughput in MiB/s. null = RDS default. Same tiering as postgres:
  # below 400 GB RDS forbids setting iops/storage_throughput at all; at
  # >=400 GB the free baseline is 12000 IOPS / 500 MiB/s and those are the
  # legal minimums (see modules/rds-postgres and the postgres soak scenario).
  type    = number
  default = null
}
