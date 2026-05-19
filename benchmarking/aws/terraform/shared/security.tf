resource "aws_security_group" "runner" {
  name        = "${local.name_prefix}-runner"
  description = "Runner EC2 — egress for SSM + source connections"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "load_gen" {
  name        = "${local.name_prefix}-load-gen"
  description = "Load-gen EC2 — egress for SSM + source writes"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Stacks reference these to allow ingress to their RDS / MSK / etc.
output "runner_sg_id"   { value = aws_security_group.runner.id }
output "load_gen_sg_id" { value = aws_security_group.load_gen.id }
