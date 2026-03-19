output "region" {
  value = var.region
}

output "bucket_name" {
  value = aws_s3_bucket.warehouse.id
}

output "role_arn" {
  value = aws_iam_role.polaris_vending.arn
}
