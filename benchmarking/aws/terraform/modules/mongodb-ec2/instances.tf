resource "aws_instance" "mongod" {
  ami           = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type = var.instance_type
  subnet_id     = var.subnet_ids[0]
  # Public IP for internet egress (install mongodb-org + register SSM). The
  # caller places this box in a public subnet (see the stack); the shared VPC
  # has no NAT gateway, so a public IP + IGW is the only egress path. Ingress is
  # still gated by the SG below (27017 from bench clients only).
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.mongod.id]
  iam_instance_profile        = var.iam_instance_profile

  # The mongod host discovers its own private IP from IMDS at boot (see
  # user-data.tftpl) rather than receiving a static IP: the instance's private
  # IP is not known at plan time, so it can't be templated in. rs.initiate()
  # runs on the box with its own IP as the member host, and clients connect to
  # that same IP via the mongodb_dsn output.
  user_data = templatefile("${path.module}/user-data.tftpl", {
    db_name         = var.db_name
    mongodb_version = var.mongodb_version
  })

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    throughput  = 250
    iops        = 3000
  }

  tags = {
    Name = "${var.name_prefix}-mongod"
    Role = "mongodb-source"
  }
}
