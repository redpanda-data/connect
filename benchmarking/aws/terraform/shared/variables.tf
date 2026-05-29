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
  default     = "c8g.large"
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

variable "orphan_ttl_hours" {
  description = "How long a tagged bench resource can live before the cleanup Lambda destroys it. Bumped to 4 because the postgres 4-vCPU sweep with both engines takes ~2.5-3 hours and was tripping the TTL mid-bench on long runs (2026-05-29)."
  type        = number
  default     = 4
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
