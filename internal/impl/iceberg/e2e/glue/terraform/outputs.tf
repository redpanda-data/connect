output "bucket_name" {
  description = "S3 warehouse bucket name"
  value       = aws_s3_bucket.warehouse.id
}

output "database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.iceberg.name
}

output "region" {
  description = "AWS region"
  value       = var.region
}

output "glue_catalog_url" {
  description = "Glue REST catalog endpoint"
  value       = "https://glue.${var.region}.amazonaws.com/iceberg"
}

output "glue_warehouse" {
  description = "Glue catalog warehouse (AWS account ID)"
  value       = aws_glue_catalog_database.iceberg.catalog_id
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.iceberg.name
}

output "athena_results_bucket" {
  description = "Athena results bucket name"
  value       = aws_s3_bucket.athena_results.id
}

output "config_file" {
  description = "Path to rendered example config"
  value       = local_file.example_config.filename
}
