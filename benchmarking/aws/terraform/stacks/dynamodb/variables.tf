variable "region" {
  type    = string
  default = "us-east-2"
}

variable "table_name" {
  type    = string
  default = "rpcn_bench_ddb_orders"
}

variable "read_capacity" {
  type    = number
  default = 1000
}

variable "write_capacity" {
  type    = number
  default = 10000
}
