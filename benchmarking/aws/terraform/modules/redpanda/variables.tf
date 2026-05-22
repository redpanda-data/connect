variable "name_prefix" {
  description = "Resource name prefix (matches shared stack)."
  type        = string
}

variable "cluster_size" {
  description = "Number of brokers. Static list of broker_ips must match this length."
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "EC2 instance type per broker. Default Graviton ARM with NVMe instance store."
  type        = string
  default     = "im4gn.2xlarge"
}

variable "vpc_id" {
  description = "VPC the brokers live in."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for broker placement (length must >= cluster_size)."
  type        = list(string)
}

variable "broker_ips" {
  description = "Static private IPs for brokers, one per broker. Must be within the matching subnet."
  type        = list(string)
}

variable "iam_instance_profile" {
  description = "IAM instance profile name attached to each broker (for SSM access)."
  type        = string
}

variable "allowed_client_sgs" {
  description = "Security groups permitted to reach Kafka (9092) and the admin/metrics endpoint (9644)."
  type        = list(string)
}
