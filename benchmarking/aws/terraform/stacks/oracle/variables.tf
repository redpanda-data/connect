variable "region" {
  type    = string
  default = "us-east-2"
}
variable "instance_class" { type = string }
variable "storage_gb" { type = number }
variable "iops" { type = number }
# MiB/s of gp3 storage throughput. Defaulted so scenarios that predate this
# variable keep the RDS default they were measured on; raise it in a scenario
# to lift the throughput ceiling (iops must be >= 4x this value).
variable "storage_throughput" {
  type    = number
  default = 500
}
variable "parameters" {
  type    = map(string)
  default = {}
}
