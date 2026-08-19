terraform {
  required_version = ">= 1.6"
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
      Stack     = "postgres"
      ManagedBy = "terraform"
      # The cleanup lambda derives creation time for RDS subnet/parameter
      # groups and security groups EXCLUSIVELY from this tag (see
      # cleanup-lambda/sweep.go sessionCreatedAt) — those resources carry no
      # creation-time field of their own. Without it, an aborted teardown
      # leaves this stack's groups unsweepable forever, and the surviving
      # security group blocks the shared VPC's deletion on every sweep.
      "bench-session-id" = var.bench_session_id
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
  source      = "../../modules/rds-postgres"
  name_prefix = "rpcn-bench-pg"
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
  iam_auth_enabled   = var.iam_auth_enabled
}

data "aws_caller_identity" "current" {}

# Lets the runner/load-gen hosts mint IAM auth tokens for the bench_iam DB
# user. Attached to shared's bench-host role (exported for exactly this
# purpose) and scoped to this one instance's resource-id + user — NOT
# rds-db:* on * — so the PR-mode threat model ("untrusted binary runs under
# the instance role") gains only the ability to log into the bench database
# it is already benchmarking.
resource "aws_iam_role_policy" "rds_iam_connect" {
  count = var.iam_auth_enabled ? 1 : 0
  name  = "rpcn-bench-pg-iam-connect"
  role  = data.terraform_remote_state.shared.outputs.bench_host_role_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["rds-db:connect"]
      Resource = ["arn:aws:rds-db:${var.region}:${data.aws_caller_identity.current.account_id}:dbuser:${module.rds.db_resource_id}/bench_iam"]
    }]
  })
}
