resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-pg"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "this" {
  name        = "${var.name_prefix}-pg-sg"
  description = "Allow Postgres from bench clients"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.client_sg_ids
    content {
      from_port       = 5432
      to_port         = 5432
      protocol        = "tcp"
      security_groups = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_parameter_group" "this" {
  name   = "${var.name_prefix}-pg"
  family = "postgres16"
  dynamic "parameter" {
    for_each = var.parameters
    content {
      name         = parameter.key
      value        = parameter.value
      apply_method = "pending-reboot"
    }
  }
}

resource "random_password" "master" {
  length  = 20
  special = false
}

resource "aws_db_instance" "this" {
  identifier             = "${var.name_prefix}-pg"
  engine                 = "postgres"
  engine_version         = var.engine_version
  instance_class         = var.instance_class
  allocated_storage      = var.storage_gb
  storage_type           = "gp3"
  iops                   = var.iops
  storage_throughput     = var.storage_throughput
  db_name                = var.db_name
  username               = var.master_username
  password               = random_password.master.result
  parameter_group_name   = aws_db_parameter_group.this.name
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.this.id]
  skip_final_snapshot    = true
  deletion_protection    = false
  publicly_accessible    = false
  apply_immediately      = true

  # IAM DB auth is the soak profile's credential-rotation lever: RDS IAM
  # tokens live ~15 minutes, so a sustained run with iam_auth_enabled
  # exercises exactly the token-expiry window behind #4668/#4258. Free to
  # leave enabled; password auth keeps working alongside it.
  iam_database_authentication_enabled = var.iam_auth_enabled
}
