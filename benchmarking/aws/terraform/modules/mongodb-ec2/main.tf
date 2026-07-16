terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
}

# Same AL2023 Graviton AMI the redpanda brokers use. Keeps the mongod host on
# the same kernel/arch as the rest of the bench fleet (all c8g/im4gn Graviton).
data "aws_ssm_parameter" "al2023_arm64_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}
