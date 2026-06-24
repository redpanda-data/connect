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
  source         = "../../modules/rds-postgres"
  name_prefix    = "rpcn-bench-pg"
  vpc_id         = data.terraform_remote_state.shared.outputs.vpc_id
  subnet_ids     = data.terraform_remote_state.shared.outputs.private_subnet_ids
  client_sg_ids  = [
    data.terraform_remote_state.shared.outputs.runner_sg_id,
    data.terraform_remote_state.shared.outputs.load_gen_sg_id,
  ]
  instance_class = var.instance_class
  storage_gb     = var.storage_gb
  iops           = var.iops
  parameters     = var.parameters
}
