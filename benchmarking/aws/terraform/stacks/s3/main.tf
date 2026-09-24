terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "redpanda-connect-bench"
      Stack     = "s3"
      ManagedBy = "terraform"
    }
  }
}

# The s3 sink bench needs zero connector-specific resources: it reuses the
# shared stack's results_bucket and redpanda_broker_endpoints outputs
# (raw objects land under raw/<topic>/ in that same bucket — see
# sinkspec_s3.go), plus aws_region, which the runner injects directly rather
# than reading from any Terraform output. This stack exists only so
# `stack: s3` resolves to a real terraform apply/destroy pair; it applies 0
# resources.
