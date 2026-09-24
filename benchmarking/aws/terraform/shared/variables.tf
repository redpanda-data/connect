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

  # A scenario can override this default via infra.load_gen.instance_type
  # (see runner/scenario.go's LoadGenSpec) without touching the default here.
  # Scenarios whose target byte rate approaches c8g.large's *sustained*
  # network baseline (0.937 Gbps / 117 MB/s) must set it: s3/orders-live.yaml
  # targets 360 MB/s (3.07x that baseline) and was silently network-shaped by
  # EC2 burst credits, capping delivered throughput with no error or log line
  # once the credits drained.
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
  # Deliberately high in each /24. The runner and load generator share
  # aws_subnet.public[0] with these brokers but take DYNAMIC addresses, and AWS
  # allocates those from the bottom of the range. The previous defaults
  # (.10/.11) sat in that allocation path and a launch failed with
  # `InvalidIPAddress.InUse` on broker[2] (10.42.0.11) after a dynamic
  # instance was handed that address first. Keeping the static IPs at the top
  # of each subnet removes the race.
  default = ["10.42.0.200", "10.42.1.200", "10.42.0.201"]
}
