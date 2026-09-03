variable "name_prefix" {
  description = "Resource name prefix (matches shared stack)."
  type        = string
}

variable "vpc_id" {
  description = "VPC the mongod host lives in."
  type        = string
}

variable "subnet_ids" {
  description = <<-EOT
    Subnet IDs; the host is placed in the first one. Must be PUBLIC subnets:
    the mongod box needs internet egress (mongodb-org install + SSM
    registration) and the shared VPC has no NAT gateway, so egress is only
    available via the public subnets' internet gateway.
  EOT
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) > 0
    error_message = "subnet_ids must contain at least one subnet for the mongod host."
  }
}

variable "client_sg_ids" {
  description = "Security groups permitted to reach mongod on 27017 (runner + load-gen)."
  type        = list(string)
}

variable "iam_instance_profile" {
  description = "IAM instance profile name attached to the host (for SSM access + S3 seeder self-stage during reset)."
  type        = string
}

variable "instance_type" {
  description = <<-EOT
    EC2 instance type. Default is a Graviton NVMe-instance-store family so the
    mongod dbPath sits on local NVMe (highest sustained write throughput, matching
    the redpanda module's storage strategy). im4gn.2xlarge exposes 1x /dev/nvme1n1.
    If you switch to a non-NVMe family, the user-data mkfs/mount step will fail —
    adjust user-data.tftpl accordingly.
  EOT
  type        = string
  default     = "im4gn.2xlarge"
}

variable "db_name" {
  description = "Database created for the bench collections."
  type        = string
  default     = "benchdb"
}

variable "mongodb_version" {
  description = "mongodb-org major version series to install from the official repo."
  type        = string
  default     = "7.0"
}
