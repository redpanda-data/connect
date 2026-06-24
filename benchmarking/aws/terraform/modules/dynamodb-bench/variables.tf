variable "table_name" {
  type        = string
  description = "Name of the bench source table. The reset hook drops + recreates this table between sweep points, so a stable name is fine."
}

variable "read_capacity" {
  type        = number
  default     = 1000
  description = "Provisioned RCU. snapshot_mode=none on the connector means RCU is only consumed by the connector's DescribeTable/ListStreams polling; a small floor is enough."
}

variable "write_capacity" {
  type        = number
  default     = 10000
  description = "Provisioned WCU. Sized for the workload's PutItem rate at the chosen item size — each item up to 1 KiB consumes 1 WCU; larger items consume ceil(size/1KiB) WCU. Bump in the scenario stack if hot-partition throttling shows up in load-gen logs."
}
