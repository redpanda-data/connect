output "mssql_dsn" {
  value     = module.rds.mssql_dsn
  sensitive = true
}
output "mssql_master_dsn" {
  value     = module.rds.mssql_master_dsn
  sensitive = true
}
output "mssql_endpoint" { value = module.rds.mssql_endpoint }
output "mssql_host" { value = module.rds.mssql_host }
output "mssql_port" { value = module.rds.mssql_port }
output "mssql_user" { value = module.rds.mssql_user }
output "mssql_db" { value = module.rds.mssql_db }
output "mssql_password" {
  value     = module.rds.mssql_password
  sensitive = true
}
