output "postgres_dsn" {
  value     = "postgres://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.address}:5432/${var.db_name}?sslmode=require"
  sensitive = true
}
output "postgres_endpoint" { value = aws_db_instance.this.address }
output "postgres_password" {
  value     = random_password.master.result
  sensitive = true
}
