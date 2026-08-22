# mongodb_dsn is the connection string both the seeder (MONGODB_DSN env) and the
# Connect mongodb_cdc `url` field consume. replicaSet=rs0 makes the driver do
# replica-set topology discovery, which is required for change streams. The RS
# member advertises the same private IP (see user-data rs.initiate), so a client
# connecting to this address discovers a matching member and streams.
output "mongodb_dsn" {
  value = "mongodb://${aws_instance.mongod.private_ip}:27017/?replicaSet=rs0"
}

output "mongodb_endpoint" {
  value = aws_instance.mongod.private_ip
}

# Discrete parts drive the KC (Debezium MongoDB) render in buildKCRenderInputs,
# which reads engineSpec.Reset*OutputKey. mongod runs without auth, so user and
# password are empty and the KC connection-string template omits credentials.
output "mongodb_host" { value = aws_instance.mongod.private_ip }
output "mongodb_port" { value = "27017" }
output "mongodb_user" { value = "" }
output "mongodb_password" {
  value     = ""
  sensitive = true
}
output "mongodb_db" { value = var.db_name }
