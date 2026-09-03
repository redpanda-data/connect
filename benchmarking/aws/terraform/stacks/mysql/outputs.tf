output "mysql_dsn" {
  value     = module.rds.mysql_dsn
  sensitive = true
}
output "mysql_endpoint" { value = module.rds.mysql_endpoint }
output "mysql_host" { value = module.rds.mysql_host }
output "mysql_port" { value = module.rds.mysql_port }
output "mysql_user" { value = module.rds.mysql_user }
output "mysql_db" { value = module.rds.mysql_db }
output "mysql_password" {
  value     = module.rds.mysql_password
  sensitive = true
}
