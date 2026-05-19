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
      Project          = "redpanda-connect-bench"
      "bench-session-id" = var.bench_session_id
      ManagedBy        = "terraform"
    }
  }
}

locals {
  name_prefix = "rpcn-bench"
}
