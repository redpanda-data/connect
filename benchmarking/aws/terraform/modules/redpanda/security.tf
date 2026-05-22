resource "aws_security_group" "broker" {
  name        = "${var.name_prefix}-redpanda"
  description = "Redpanda broker cluster - kafka, admin, RPC"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Kafka API from allowed clients
resource "aws_security_group_rule" "broker_kafka_ingress" {
  count                    = length(var.allowed_client_sgs)
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9092
  protocol                 = "tcp"
  source_security_group_id = var.allowed_client_sgs[count.index]
  security_group_id        = aws_security_group.broker.id
  description              = "Kafka from client SG"
}

# Admin / Prometheus metrics from allowed clients
resource "aws_security_group_rule" "broker_admin_ingress" {
  count                    = length(var.allowed_client_sgs)
  type                     = "ingress"
  from_port                = 9644
  to_port                  = 9644
  protocol                 = "tcp"
  source_security_group_id = var.allowed_client_sgs[count.index]
  security_group_id        = aws_security_group.broker.id
  description              = "Admin/metrics from client SG"
}

# Broker-to-broker RPC (raft)
resource "aws_security_group_rule" "broker_rpc_self" {
  type                     = "ingress"
  from_port                = 33145
  to_port                  = 33145
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.broker.id
  security_group_id        = aws_security_group.broker.id
  description              = "Broker RPC (raft)"
}
