output "mysql_dsn" {
  # go-sql-driver/mysql DSN: user:pass@tcp(host:port)/db?params
  # parseTime=true maps DATETIME → time.Time at the driver layer.
  # tls=skip-verify because the RDS-internal CA isn't in the runner image.
  value     = "${var.master_username}:${random_password.master.result}@tcp(${aws_db_instance.this.address}:3306)/${var.db_name}?parseTime=true&tls=skip-verify"
  sensitive = true
}
output "mysql_endpoint" { value = aws_db_instance.this.address }
output "mysql_host" { value = aws_db_instance.this.address }
output "mysql_port" { value = "3306" }
output "mysql_user" { value = var.master_username }
output "mysql_db" { value = var.db_name }
output "mysql_password" {
  value     = random_password.master.result
  sensitive = true
}
