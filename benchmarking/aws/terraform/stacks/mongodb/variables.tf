variable "region" {
  type    = string
  default = "us-east-2"
}

# Unlike the RDS stacks (instance_class/storage_gb/iops), the self-hosted mongod
# host takes a single EC2 instance_type. The default is a Graviton NVMe family;
# the scenario's infra.source.instance_type overrides it.
variable "instance_type" {
  type    = string
  default = "im4gn.2xlarge"
}
