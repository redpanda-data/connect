terraform {
  required_version = ">= 1.10" # S3-native state locking (use_lockfile in backend.hcl)
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project            = "redpanda-connect-bench"
      "bench-session-id" = var.bench_session_id
      ManagedBy          = "terraform"
      # A live bench crossing the org cloud-nuke's ~02:25 UTC sweep must
      # not be terminated mid-run. OUR reaper still owns cleanup — it
      # keys on Project, not this tag, so the 4h TTL is unaffected.
      "cloud-nuke-excluded" = "true"
    }
  }
}

locals {
  name_prefix = "rpcn-bench"
}
