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

  # CRITICAL: backup_retention_period > 0 is what ENABLES binlog on RDS
  # MySQL at all — with backups off there is no binlog and mysql_cdc has
  # nothing to read. It does NOT retain the binlog: RDS purges binlog files
  # as soon as they're backed up unless 'binlog retention hours' is set,
  # and that is a runtime stored procedure
  # (CALL mysql.rds_set_configuration), not a parameter-group knob, so the
  # scenarios' reset steps call it on every run (see scenarios/mysql/).
  backup_retention_period = 1
  # Pin the backup window to off-hours UTC. Without this, RDS picks a random
  # daily slot; the 2026-05-21 smoke saw the 8 vCPU sweep point degrade from
  # ~100 MB/s to ~60 MB/s mid-window because a backup overlapped (gp3
  # throughput is shared between user writes and snapshot copy). 06:00-08:00
  # UTC is well before US-Pacific working hours when benches typically run,
  # and ends before the nightly soak's 08:10 UTC cron even fires — a nightly
  # soak instance (created ~08:15, destroyed ~2.5h later) never lives through
  # the window at all, only through the unavoidable on-creation snapshot,
  # which lands during provisioning rather than the measured run.
  backup_window = "06:00-08:00"
}
