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
      Stack     = "snowflake"
      ManagedBy = "terraform"
    }
  }
}

# The Snowflake account is external to AWS — this stack provisions nothing.
# It resolves the non-secret connection facts from SSM parameters (created
# once by hand, see scenarios/snowflake/README.md) into TF outputs, so the
# runner's ${SNOWFLAKE_*} placeholder substitution works unchanged.
#
# The private key deliberately does NOT flow through here: everything a data
# source reads lands in TF state in S3. The runner host instead fetches the
# /bench/snowflake/private_key SecureString directly at reset time, using the
# ssm:GetParameter its AmazonSSMManagedInstanceCore role policy grants.
data "aws_ssm_parameter" "account" { name = "/bench/snowflake/account" }
data "aws_ssm_parameter" "user" { name = "/bench/snowflake/user" }
data "aws_ssm_parameter" "role" { name = "/bench/snowflake/role" }
data "aws_ssm_parameter" "database" { name = "/bench/snowflake/database" }
data "aws_ssm_parameter" "schema" { name = "/bench/snowflake/schema" }
