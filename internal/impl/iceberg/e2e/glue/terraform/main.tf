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

# --- S3 ---

resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.prefix}-iceberg-e2e"
  force_destroy = true
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.prefix}-iceberg-e2e-athena-results"
  force_destroy = true
}

# --- Glue ---

resource "aws_glue_catalog_database" "iceberg" {
  name         = replace("${var.prefix}_iceberg_e2e", "-", "_")
  location_uri = "s3://${aws_s3_bucket.warehouse.id}/"
}

# --- Athena ---

resource "aws_athena_workgroup" "iceberg" {
  name          = replace("${var.prefix}_iceberg_e2e", "-", "_")
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/results/"
    }
    enforce_workgroup_configuration = true
  }
}

# --- Rendered example config ---

resource "local_file" "example_config" {
  filename = "${path.module}/example-config.yaml"
  content = templatefile("${path.module}/templates/example-config.yaml.tftpl", {
    glue_catalog_url = "https://glue.${var.region}.amazonaws.com/iceberg"
    warehouse        = aws_glue_catalog_database.iceberg.catalog_id
    bucket_name      = aws_s3_bucket.warehouse.id
    region           = var.region
    database_name    = aws_glue_catalog_database.iceberg.name
  })
}
