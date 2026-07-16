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
      Stack     = "mongodb"
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

module "mongodb" {
  source      = "../../modules/mongodb-ec2"
  name_prefix = "rpcn-bench-mongo"
  vpc_id      = data.terraform_remote_state.shared.outputs.vpc_id
  # PUBLIC subnets, not private: unlike the RDS stacks (whose managed instances
  # need no egress), the self-hosted mongod box must reach the internet to
  # install mongodb-org and register with SSM. The shared VPC has NO NAT gateway,
  # so private-subnet instances have zero egress — the runner/brokers all run in
  # the public subnets (IGW + public IP). mongod does the same. Its SG still
  # restricts port 27017 to the bench client SGs, so a public IP does not expose
  # MongoDB to the internet.
  subnet_ids = data.terraform_remote_state.shared.outputs.public_subnet_ids
  client_sg_ids = [
    data.terraform_remote_state.shared.outputs.runner_sg_id,
    data.terraform_remote_state.shared.outputs.load_gen_sg_id,
  ]
  iam_instance_profile = data.terraform_remote_state.shared.outputs.bench_host_instance_profile
  instance_type        = var.instance_type
}
