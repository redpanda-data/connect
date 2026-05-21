resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-my"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "this" {
  name        = "${var.name_prefix}-my-sg"
  description = "Allow MySQL from bench clients"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.client_sg_ids
    content {
      from_port       = 3306
      to_port         = 3306
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
  name   = "${var.name_prefix}-my"
  family = "mysql8.0"
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
  identifier             = "${var.name_prefix}-my"
  engine                 = "mysql"
  engine_version         = var.engine_version
  instance_class         = var.instance_class
  allocated_storage      = var.storage_gb
  storage_type           = "gp3"
  iops                   = var.iops
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

  # CRITICAL: RDS purges binlog immediately if backups are off. CDC needs the
  # binlog to be retained long enough for the connector to read it. 1 day is
  # the minimum that keeps binlog around between writes and reads.
  backup_retention_period = 1
}
