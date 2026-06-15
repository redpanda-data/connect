variable "table_names" {
  type        = list(string)
  description = "Names of the bench source tables. One DynamoDB table is created per name (for_each). Multiple tables let the workload exceed the per-table 40K WCU quota: total provisioned WCU = length(table_names) * write_capacity, while each individual table stays within its own per-table quota. The reset hook only drops the connector's checkpoint table between sweep points, so stable names are fine."
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
