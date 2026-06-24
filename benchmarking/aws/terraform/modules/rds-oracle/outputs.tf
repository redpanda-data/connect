output "oracle_dsn" {
  # go-ora (sijms/go-ora) connection URL: oracle://user:pass@host:port/service.
  # The service name is the RDS Oracle SID (var.db_name). No TLS params — RDS
  # Oracle accepts plaintext on 1521 by default (no option group configured),
  # so unlike postgres/mysql there's no skip-verify gymnastics.
  value     = "oracle://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.address}:1521/${var.db_name}"
  sensitive = true
}
output "oracle_endpoint" { value = aws_db_instance.this.address }
output "oracle_host" { value = aws_db_instance.this.address }
output "oracle_port" { value = "1521" }
output "oracle_user" { value = var.master_username }
output "oracle_db" { value = var.db_name }
output "oracle_password" {
  value     = random_password.master.result
  sensitive = true
}
