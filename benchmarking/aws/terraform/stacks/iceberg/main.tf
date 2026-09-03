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
      Stack     = "iceberg"
      ManagedBy = "terraform"
    }
  }
}

module "iceberg" {
  source      = "../../modules/glue-iceberg"
  name_prefix = "rpcn-bench-ice"
  region      = var.region
}
