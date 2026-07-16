output "mongodb_dsn" {
  value = module.mongodb.mongodb_dsn
}
output "mongodb_endpoint" {
  value = module.mongodb.mongodb_endpoint
}
output "mongodb_host" { value = module.mongodb.mongodb_host }
output "mongodb_port" { value = module.mongodb.mongodb_port }
output "mongodb_user" { value = module.mongodb.mongodb_user }
output "mongodb_password" {
  value     = module.mongodb.mongodb_password
  sensitive = true
}
output "mongodb_db" { value = module.mongodb.mongodb_db }
