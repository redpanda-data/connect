output "oracle_dsn" {
  value     = module.rds.oracle_dsn
  sensitive = true
}
output "oracle_endpoint" { value = module.rds.oracle_endpoint }
output "oracle_host" { value = module.rds.oracle_host }
output "oracle_port" { value = module.rds.oracle_port }
output "oracle_user" { value = module.rds.oracle_user }
output "oracle_db" { value = module.rds.oracle_db }
output "oracle_password" {
  value     = module.rds.oracle_password
  sensitive = true
}
