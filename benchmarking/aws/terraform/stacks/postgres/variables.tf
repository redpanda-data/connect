variable "region" {
  type    = string
  default = "us-east-2"
}
variable "instance_class" { type = string }
variable "storage_gb"     { type = number }
variable "iops"           { type = number }
variable "parameters" {
  type    = map(string)
  default = { wal_level = "logical", max_wal_senders = "20" }
}
