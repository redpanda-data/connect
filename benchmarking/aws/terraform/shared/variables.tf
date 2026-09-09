variable "region" {
  type    = string
  default = "us-east-2"
}

variable "runner_instance_type" {
  description = "EC2 instance type for the Connect benchmark host."
  type        = string
  default     = "c8g.4xlarge"
}

variable "load_gen_instance_type" {
  description = "EC2 instance type for the load generator."
  type        = string
  # MEASURED, do not "fix" this again without new evidence: the load generator's
  # instance size is NOT what limits delivered write throughput.
  #
  # On the 2026-08-07 SQL Server runs, c8g.large (2 vCPU) committed 9,198,178
  # rows over a point and c8g.4xlarge (16 vCPU) committed 8,498,328 — slightly
  # FEWER, at an identical ~10-11K rows/s against a 150K target. An 8x vCPU
  # increase changed nothing, which rules out client CPU, TLS cost and client
  # network. The arithmetic puts the constraint server-side: ~3s per 1000-row
  # insert regardless of client size, with a recurring sawtooth down to ~3K
  # rows/s that looks like a checkpoint or log-growth stall.
  #
  # Kept at c8g.large so every bench isn't paying 8x for a box that measurably
  # buys nothing. Raise it only if a specific scenario proves it client-bound.
  default = "c8g.large"
}

variable "bench_session_id" {
  description = "Tag applied to every resource for orphan cleanup. Empty string default lets `runner down` destroy without re-passing the original session id; the tag value doesn't matter during destroy."
  type        = string
  default     = ""
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "redpanda_instance_type" {
  description = "EC2 instance type per Redpanda broker."
  type        = string
  default     = "im4gn.2xlarge"
}

variable "redpanda_broker_ips" {
  description = "Static private IPs for Redpanda brokers (must fall inside the public subnets' CIDRs — brokers run in public subnets for outbound install access; the broker SG still gates inbound)."
  type        = list(string)
  default     = ["10.42.0.10", "10.42.1.10", "10.42.0.11"]
}
