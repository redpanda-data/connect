variable "name_prefix" { type = string }
variable "vpc_id"      { type = string }
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
  type    = map(string)
  # rds.logical_replication=1 is the RDS-specific knob that makes RDS set
  # wal_level=logical for us (wal_level itself isn't user-settable on RDS).
  default = { "rds.logical_replication" = "1", max_wal_senders = "20" }
}
