variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" {
  type = list(string)
}
variable "client_sg_ids" {
  type        = list(string)
  description = "SGs allowed to connect on 1521"
}
variable "instance_class" {
  # RDS Oracle does not offer Graviton classes — x86 only (db.r5/m5/t3).
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
variable "engine_version" {
  # Oracle 19c Standard Edition 2 (non-CDB). Verify a currently-offered RU with:
  #   aws rds describe-db-engine-versions --engine oracle-se2 --query \
  #     'DBEngineVersions[].EngineVersion'
  type    = string
  default = "19.0.0.0.ru-2024-10.rur-2024-10.r1"
}
variable "parameter_group_family" {
  type    = string
  default = "oracle-se2-19"
}
variable "db_name" {
  # Oracle SID: 1-8 alphanumeric chars.
  type    = string
  default = "ORCL"
}
variable "master_username" {
  type    = string
  default = "bench"
}
variable "parameters" {
  # No parameter-group settings are required for LogMiner CDC on RDS Oracle:
  # ARCHIVELOG follows backup retention, and supplemental logging is enabled
  # post-create via rdsadmin (see the cdc-rows-oracle seeder).
  type    = map(string)
  default = {}
}
