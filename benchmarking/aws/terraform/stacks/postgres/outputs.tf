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
# Passwordless: the connector's aws.enabled token builder supplies the
# password. Not sensitive — it contains no secret.
output "postgres_iam_dsn" {
  value = module.rds.postgres_iam_dsn
}
# Exposed so scenarios can reference ${REGION} (e.g. postgres_cdc's
# aws.region) instead of hardcoding the bench account's region.
output "region" {
  value = var.region
}
