data "aws_caller_identity" "current" {}

# Iceberg warehouse: data + metadata files. force_destroy so `terraform destroy`
# can reclaim it even though the engines write objects into it during a run.
resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.name_prefix}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

# Iceberg namespace. Per-engine tables are auto-created inside this database by
# Connect's iceberg output / the KC Tabular sink at runtime — not by Terraform.
# Glue DeleteDatabase cascades to its tables, so destroy works with tables present.
resource "aws_glue_catalog_database" "bench" {
  name = var.database_name
}
