# Role for the bench-catalog app on Google Cloud Run. No static AWS keys: the
# app fetches a Google-signed ID token from the metadata server and calls
# AssumeRoleWithWebIdentity. accounts.google.com is a built-in federated
# provider, so no aws_iam_openid_connect_provider is needed.
#
# Created only once the service account's unique ID is known (see README).
locals {
  bench_catalog_app_enabled = var.bench_catalog_gcp_sa_unique_id != ""
}

resource "aws_iam_role" "bench_catalog_app" {
  count                = local.bench_catalog_app_enabled ? 1 : 0
  name                 = "redpanda-connect-bench-catalog-app"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = "accounts.google.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "accounts.google.com:sub" = var.bench_catalog_gcp_sa_unique_id
          # For Google ID tokens AWS maps the azp claim to aud, and azp is the
          # service account's unique ID; the requested audience is oaud.
          "accounts.google.com:aud"  = var.bench_catalog_gcp_sa_unique_id
          "accounts.google.com:oaud" = var.bench_catalog_gcp_audience
        }
      }
    }]
  })
}

# Reads catalogue entries and writes plan objects; cannot delete or overwrite
# anything outside plans/.
resource "aws_iam_role_policy" "bench_catalog_app" {
  count = local.bench_catalog_app_enabled ? 1 : 0
  name  = "catalog-and-plans"
  role  = aws_iam_role.bench_catalog_app[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Unconditional on purpose: without ListBucket S3 answers a missing key
        # with 403, not 404, and the plan store relies on 404 for the absent
        # plans/<id>.dispatched/.run/.done markers. GetObject carries no
        # s3:prefix, so a prefix condition would not help here either.
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.bench_catalog.arn
      },
      {
        Effect = "Allow"
        Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.bench_catalog.arn}/catalog/v1/*",
          "${aws_s3_bucket.bench_catalog.arn}/plans/*",
        ]
      },
      {
        Effect = "Allow"
        Action = "s3:PutObject"
        Resource = [
          "${aws_s3_bucket.bench_catalog.arn}/plans/*.json",
          "${aws_s3_bucket.bench_catalog.arn}/plans/*.dispatched",
        ]
        # Create only: S3 refuses a put without If-None-Match, so a plan
        # can't be rewritten after it was approved and claimed, and a claim
        # marker can't be replaced. The app's plan store always sends
        # If-None-Match: * for these keys.
        Condition = {
          Null = { "s3:if-none-match" = "false" }
        }
      },
    ]
  })
}
