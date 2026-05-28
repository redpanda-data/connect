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
  description = "How long a tagged bench resource can live before the cleanup Lambda destroys it."
  type        = number
  default     = 3
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
