output "postgres_dsn" {
  value     = "postgres://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.address}:5432/${var.db_name}?sslmode=require"
  sensitive = true
}
output "postgres_endpoint" { value = aws_db_instance.this.address }

# Passwordless DSN for the IAM-auth user: postgres_cdc's aws.enabled token
# builder supplies the password at connect time. Emitted unconditionally —
# it is only meaningful when iam_auth_enabled and the reset has created the
# role, but rendering it costs nothing and keeps the output set stable.
output "postgres_iam_dsn" {
  value = "postgres://${var.iam_username}@${aws_db_instance.this.address}:5432/${var.db_name}?sslmode=require"
}

# DbiResourceId, the ARN component rds-db:connect policies are scoped by
# (arn:aws:rds-db:<region>:<acct>:dbuser:<resource-id>/<user>).
output "db_resource_id" { value = aws_db_instance.this.resource_id }
output "postgres_password" {
  value     = random_password.master.result
  sensitive = true
}
