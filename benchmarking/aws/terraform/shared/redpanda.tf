module "redpanda" {
  source = "../modules/redpanda"

  name_prefix          = local.name_prefix
  cluster_size         = 3
  instance_type        = var.redpanda_instance_type
  vpc_id = aws_vpc.main.id
  # Brokers live in PUBLIC subnets so cloud-init can reach packages.redpanda.com
  # and so the SSM agent can register with AWS endpoints. The broker security
  # group still restricts ingress to runner + load-gen SGs only — being in a
  # public subnet does NOT mean public access. Private subnets would require
  # a NAT gateway for outbound install, which is not justified for a bench host.
  subnet_ids           = aws_subnet.public[*].id
  broker_ips           = var.redpanda_broker_ips
  iam_instance_profile = aws_iam_instance_profile.bench_host.name
  allowed_client_sgs   = [aws_security_group.runner.id, aws_security_group.load_gen.id]
}
