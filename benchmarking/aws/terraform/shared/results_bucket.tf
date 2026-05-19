resource "aws_s3_bucket" "results" {
  bucket_prefix = "${local.name_prefix}-results-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    id     = "expire-raw-json"
    status = "Enabled"
    expiration { days = 180 }
  }
}
