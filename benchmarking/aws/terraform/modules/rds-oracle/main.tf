resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-ora"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "this" {
  name        = "${var.name_prefix}-ora-sg"
  description = "Allow Oracle from bench clients"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.client_sg_ids
    content {
      from_port       = 1521
      to_port         = 1521
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
  name   = "${var.name_prefix}-ora"
  family = var.parameter_group_family
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
  identifier     = "${var.name_prefix}-ora"
  engine         = "oracle-se2"
  engine_version = var.engine_version
  # RDS Oracle has NO Graviton (arm64) instance support — must be x86 (db.r5/m5),
  # same constraint as SQL Server. The c8g runner is a separate EC2 host.
  instance_class = var.instance_class
  license_model  = "license-included"

  allocated_storage = var.storage_gb
  storage_type      = "gp3"
  iops              = var.iops

  # On RDS Oracle the db_name IS the Oracle SID (1-8 alphanumeric chars). go-ora
  # and Debezium both connect using it as the service name, which RDS registers
  # automatically for the non-CDB 19c architecture used by SE2.
  db_name              = var.db_name
  username             = var.master_username
  password             = random_password.master.result
  character_set_name   = "AL32UTF8"
  parameter_group_name = aws_db_parameter_group.this.name
  db_subnet_group_name = aws_db_subnet_group.this.name

  vpc_security_group_ids = [aws_security_group.this.id]
  skip_final_snapshot    = true
  deletion_protection    = false
  publicly_accessible    = false
  apply_immediately      = true

  # CRITICAL: RDS Oracle is only placed in ARCHIVELOG mode when automated backups
  # are enabled (retention > 0). LogMiner CDC reads archived redo, so this is the
  # Oracle analogue of the mysql module's binlog-retention requirement. The seeder
  # additionally raises `archivelog retention hours` via rdsadmin at first run so
  # the redo a sweep is mining isn't reaped mid-bench.
  backup_retention_period = 1
  # Pin the backup window off-hours UTC so a snapshot copy doesn't steal gp3
  # throughput from a running sweep (see rds-mysql/main.tf for the rationale).
  backup_window = "06:00-08:00"
}
