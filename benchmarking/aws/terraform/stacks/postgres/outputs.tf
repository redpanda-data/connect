output "postgres_dsn" {
  value     = module.rds.postgres_dsn
  sensitive = true
}
output "postgres_endpoint" {
  value = module.rds.postgres_endpoint
}
output "postgres_password" {
  value     = module.rds.postgres_password
  sensitive = true
}
