terraform {
  required_version = ">= 1.10" # S3-native state locking (use_lockfile in backend.hcl)
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.70" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "redpanda-connect-bench"
      Stack     = "mysql"
      ManagedBy = "terraform"
      # The cleanup lambda derives creation time for RDS subnet/parameter
      # groups and security groups EXCLUSIVELY from this tag (see
      # cleanup-lambda/sweep.go sessionCreatedAt) — those resources carry no
      # creation-time field of their own. Without it, an aborted teardown
      # leaves this stack's groups unsweepable forever, and the surviving
      # security group blocks the shared VPC's deletion on every sweep.
      "bench-session-id" = var.bench_session_id
      # See shared/main.tf: exempt from the org cloud-nuke sweep; our own
      # reaper (keyed on Project) still reaps this stack at the 4h TTL.
      "cloud-nuke-excluded" = "true"
    }
  }
}

data "terraform_remote_state" "shared" {
  backend = "s3"
  config = {
    bucket = "redpanda-connect-bench-tfstate"
    region = var.region
    key    = "shared/terraform.tfstate"
  }
}

module "rds" {
  source      = "../../modules/rds-mysql"
  name_prefix = "rpcn-bench-my"
  vpc_id      = data.terraform_remote_state.shared.outputs.vpc_id
  subnet_ids  = data.terraform_remote_state.shared.outputs.private_subnet_ids
  client_sg_ids = [
    data.terraform_remote_state.shared.outputs.runner_sg_id,
    data.terraform_remote_state.shared.outputs.load_gen_sg_id,
  ]
  instance_class     = var.instance_class
  storage_gb         = var.storage_gb
  iops               = var.iops
  storage_throughput = var.storage_throughput
  parameters         = var.parameters
}
