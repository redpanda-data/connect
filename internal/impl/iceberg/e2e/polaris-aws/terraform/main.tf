terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

# --- S3 ---

resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.prefix}-iceberg-polaris-e2e"
  force_destroy = true
}

# --- IAM role for Polaris credential vendoring ---
# Polaris assumes this role via STS:AssumeRole and vends the
# temporary credentials to REST catalog clients.

resource "aws_iam_role" "polaris_vending" {
  name                 = "${var.prefix}-polaris-vending"
  max_session_duration = 3600 # 1 hour — vended credentials expire after this

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
      }
    }]
  })
}

resource "aws_iam_role_policy" "polaris_s3" {
  name = "s3-access"
  role = aws_iam_role.polaris_vending.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
      ]
      Effect = "Allow"
      Resource = [
        aws_s3_bucket.warehouse.arn,
        "${aws_s3_bucket.warehouse.arn}/*",
      ]
    }]
  })
}
