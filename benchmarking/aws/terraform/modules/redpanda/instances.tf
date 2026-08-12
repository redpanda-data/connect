resource "aws_instance" "broker" {
  count                  = var.cluster_size
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.instance_type
  subnet_id              = var.subnet_ids[count.index % length(var.subnet_ids)]
  private_ip             = var.broker_ips[count.index]
  vpc_security_group_ids = [aws_security_group.broker.id]
  iam_instance_profile   = var.iam_instance_profile

  user_data = templatefile("${path.module}/user-data.tftpl", {
    node_id         = count.index
    self_ip         = var.broker_ips[count.index]
    seed_servers    = join(",", [for ip in var.broker_ips : "${ip}:33145"])
    advertised_kafka = "${var.broker_ips[count.index]}:9092"
    advertised_rpc  = "${var.broker_ips[count.index]}:33145"
  })

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    throughput  = 250
    iops        = 3000
  }

  tags = {
    Name = "${var.name_prefix}-redpanda-${count.index}"
    Role = "redpanda-broker"
  }
}
