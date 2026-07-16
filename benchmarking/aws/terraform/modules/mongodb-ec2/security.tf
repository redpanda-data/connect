resource "aws_security_group" "mongod" {
  name        = "${var.name_prefix}-mongod"
  description = "Self-hosted MongoDB replica-set host - mongod wire protocol"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# mongod wire protocol (27017) from the bench clients only. The host runs in a
# public subnet (for egress) with a public IP, but this SG is the sole ingress
# path and admits 27017 only from the bench client SGs — MongoDB is never
# exposed to the internet. No auth is configured on mongod itself (the bench
# measures CDC read throughput, which auth doesn't affect; both Connect and
# Debezium connect identically → fair comparison).
resource "aws_security_group_rule" "mongod_ingress" {
  count                    = length(var.client_sg_ids)
  type                     = "ingress"
  from_port                = 27017
  to_port                  = 27017
  protocol                 = "tcp"
  source_security_group_id = var.client_sg_ids[count.index]
  security_group_id        = aws_security_group.mongod.id
  description              = "mongod from bench client SG"
}
