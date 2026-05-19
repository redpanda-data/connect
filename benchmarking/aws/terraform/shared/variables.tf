variable "region" {
  type    = string
  default = "us-east-2"
}

variable "runner_instance_type" {
  description = "EC2 instance type for the Connect benchmark host."
  type        = string
  default     = "c7i.4xlarge"
}

variable "load_gen_instance_type" {
  description = "EC2 instance type for the load generator."
  type        = string
  default     = "c7i.large"
}

variable "bench_session_id" {
  description = "Tag applied to every resource for orphan cleanup."
  type        = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}
