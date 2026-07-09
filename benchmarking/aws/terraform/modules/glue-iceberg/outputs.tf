# Glue's Iceberg REST catalog endpoint. Both engines use this same URL + SigV4
# (service=glue) so the comparison is apples-to-apples.
output "glue_rest_uri" {
  value = "https://glue.${var.region}.amazonaws.com/iceberg"
}

# Glue catalog identifier = the AWS account id (the `warehouse` for the REST catalog).
output "warehouse_account_id" {
  value = data.aws_caller_identity.current.account_id
}

# Base S3 URI for Iceberg table data/metadata (no trailing slash — the runner
# appends "/" for Connect's schema_evolution.table_location).
output "warehouse_s3_uri" {
  value = "s3://${aws_s3_bucket.warehouse.bucket}/wh"
}

output "s3_bucket" {
  value = aws_s3_bucket.warehouse.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.bench.name
}
