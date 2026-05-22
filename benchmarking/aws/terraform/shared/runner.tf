data "aws_ssm_parameter" "al2023_arm64_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

locals {
  cloud_init = templatefile("${path.module}/runner-user-data.tftpl", {
    redpanda_brokers = module.redpanda.broker_endpoints
  })
}

resource "aws_instance" "runner" {
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.runner_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.runner.id]
  iam_instance_profile   = aws_iam_instance_profile.bench_host.name
  user_data              = local.cloud_init

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    throughput  = 500
    iops        = 4000
  }

  tags = { Name = "${local.name_prefix}-runner" }
}

resource "aws_instance" "load_gen" {
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.load_gen_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.load_gen.id]
  iam_instance_profile   = aws_iam_instance_profile.bench_host.name
  user_data              = local.cloud_init

  root_block_device {
    volume_type = "gp3"
    volume_size = 40
  }

  tags = { Name = "${local.name_prefix}-load-gen" }
}
