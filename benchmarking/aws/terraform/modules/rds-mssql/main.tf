resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-ms"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "this" {
  name        = "${var.name_prefix}-ms-sg"
  description = "Allow SQL Server from bench clients"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.client_sg_ids
    content {
      # 1433, not 5432/3306 (Trap: bench SGs are per-engine).
      from_port       = 1433
      to_port         = 1433
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
  name   = "${var.name_prefix}-ms"
  family = var.parameter_group_family
  # Empty by default: unlike Postgres (rds.logical_replication) and MySQL
  # (binlog_format), SQL Server CDC needs NO parameter-group changes. It is
  # turned on per-database and per-table in T-SQL by the seeder. The group
  # exists so a scenario can still pass `parameters:` for future knobs.
  dynamic "parameter" {
    for_each = var.parameters
    content {
      name         = parameter.key
      value        = parameter.value
      apply_method = "pending-reboot"
    }
  }
}

# SQL Server enforces password complexity (3 of 4 character classes) and rejects
# a master password that only happens to be, say, all-lowercase alphanumeric.
# special=false keeps the password shell-safe for the rendered seed/reset scripts
# (see the trust-boundary note in runner/scripts.go), so complexity is guaranteed
# via the min_* floors instead of by allowing punctuation.
resource "random_password" "master" {
  length      = 24
  special     = false
  min_upper   = 2
  min_lower   = 2
  min_numeric = 2
}

resource "aws_db_instance" "this" {
  identifier     = "${var.name_prefix}-ms"
  engine         = var.engine
  engine_version = var.engine_version
  instance_class = var.instance_class

  # SQL Server on RDS is license-included only (there is no BYOL path for
  # se/ee via this API), and it is x86-only — no Graviton instance classes,
  # same constraint as RDS Oracle. See references/rds-quirks.md.
  license_model = "license-included"

  allocated_storage = var.storage_gb
  storage_type      = "gp3"
  iops              = var.iops

  # NOTE: no `db_name`. RDS refuses it for every SQL Server engine — the
  # instance comes up with only the system databases and the application
  # database has to be created afterwards. The cdc-rows-mssql seeder does
  # that (CREATE DATABASE against the master DSN) as its first step, which
  # is also why this module emits BOTH mssql_master_dsn and mssql_dsn.
  username = var.master_username
  password = random_password.master.result

  parameter_group_name   = aws_db_parameter_group.this.name
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.this.id]
  skip_final_snapshot    = true
  deletion_protection    = false
  publicly_accessible    = false
  apply_immediately      = true

  # backup_retention_period > 0 puts the database in the FULL recovery model,
  # which is how customers actually run SQL Server CDC — and it is the shape
  # the capture job's log reader is tuned for. At 0 RDS switches to SIMPLE;
  # CDC still functions there but the log-truncation behaviour differs, so the
  # bench would no longer be measuring the common configuration.
  backup_retention_period = 1
  # Same reasoning as rds-mysql: pin the window off-hours so a snapshot copy
  # never lands mid-sweep and steals gp3 throughput from the write path.
  backup_window = "06:00-08:00"
}
