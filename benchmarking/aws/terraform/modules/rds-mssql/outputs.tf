// microsoft/go-mssqldb DSN: sqlserver://user:pass@host:port?database=...
//
// encrypt=true + TrustServerCertificate=true mirrors the posture of every other
// engine in the suite (postgres_cdc / mysql_cdc both run TLS with verification
// skipped, because the RDS-internal CA isn't in the runner image). Keeping TLS
// ON matters for fairness: the mssql-jdbc driver Debezium uses defaults to
// encrypt=true, so disabling it on the Connect side would hand Connect a free
// CPU saving at every pinned-vCPU sweep point.
//
// The master password is alphanumeric (random_password special=false) so it
// needs no URL escaping here.
output "mssql_dsn" {
  value     = "sqlserver://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.address}:1433?database=${var.db_name}&encrypt=true&TrustServerCertificate=true"
  sensitive = true
}

// Same server, master database. The seeder needs this because RDS creates a
// SQL Server instance with no application database (see main.tf), so CREATE
// DATABASE and `msdb.dbo.rds_cdc_enable_db` have to run before mssql_dsn is
// connectable at all. Wired in as MSSQL_MASTER_DSN via the engineSpec's
// ExtraEnvVars.
output "mssql_master_dsn" {
  value     = "sqlserver://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.address}:1433?database=master&encrypt=true&TrustServerCertificate=true"
  sensitive = true
}

output "mssql_endpoint" { value = aws_db_instance.this.address }
output "mssql_host" { value = aws_db_instance.this.address }
output "mssql_port" { value = "1433" }
output "mssql_user" { value = var.master_username }
output "mssql_db" { value = var.db_name }
output "mssql_password" {
  value     = random_password.master.result
  sensitive = true
}
