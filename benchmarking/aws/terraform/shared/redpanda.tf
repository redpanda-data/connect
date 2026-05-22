module "redpanda" {
  source = "../modules/redpanda"

  name_prefix          = local.name_prefix
  cluster_size         = 3
  instance_type        = var.redpanda_instance_type
  vpc_id               = aws_vpc.main.id
  subnet_ids           = aws_subnet.private[*].id
  broker_ips           = var.redpanda_broker_ips
  iam_instance_profile = aws_iam_instance_profile.bench_host.name
  allowed_client_sgs   = [aws_security_group.runner.id, aws_security_group.load_gen.id]
}
