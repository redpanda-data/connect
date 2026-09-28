# --- BYO catalog storage (OPTIONAL, off by default) ---
#
# Everything in this file is gated behind create_storage = true. It provisions
# an S3 bucket plus the Unity Catalog plumbing (storage credential → external
# location) so the e2e catalog can live on customer-owned storage. Needed
# when the metastore's own storage can't back the catalog:
#
#   * Databricks express-setup trials use "default storage", which does NOT
#     support credential vending for external Iceberg REST clients
#     (https://docs.databricks.com/aws/en/storage/default-storage), so the
#     tests can never talk to a catalog created on it;
#   * metastores with no storage_root at all can't create a catalog without
#     an explicit managed location.
#
# The resource shape follows the databricks provider's Unity Catalog guide
# verbatim (docs/guides/unity-catalog.md): UC storage credentials have a
# chicken-and-egg with the IAM role — the role's trust policy needs the
# credential's external ID, which only exists after the credential is
# created. The documented break: create the credential FIRST, pointing at the
# role ARN as a *constructed string* (never a resource reference, which would
# be a cycle), then build the role's trust policy from the credential's
# external_id via the databricks_aws_unity_catalog_assume_role_policy data
# source. The companion databricks_aws_unity_catalog_policy data source
# generates the S3 access policy (Get/Put/DeleteObject, ListBucket,
# GetBucketLocation + the self-assume statement UC requires).

data "aws_caller_identity" "current" {
  count = var.create_storage ? 1 : 0
}

locals {
  # IAM role and storage-credential name (they match, per the UC guide).
  uc_role_name = "${var.prefix}-databricks-e2e-uc-${random_id.suffix.hex}"
}

# --- AWS side ---

resource "aws_s3_bucket" "e2e" {
  count = var.create_storage ? 1 : 0

  bucket        = "${var.prefix}-databricks-e2e-${random_id.suffix.hex}"
  force_destroy = true # disposable: destroy removes objects too
}

data "databricks_aws_unity_catalog_assume_role_policy" "e2e" {
  count = var.create_storage ? 1 : 0

  aws_account_id = data.aws_caller_identity.current[0].account_id
  role_name      = local.uc_role_name
  external_id    = databricks_storage_credential.e2e[0].aws_iam_role[0].external_id
}

data "databricks_aws_unity_catalog_policy" "e2e" {
  count = var.create_storage ? 1 : 0

  aws_account_id = data.aws_caller_identity.current[0].account_id
  bucket_name    = aws_s3_bucket.e2e[0].id
  role_name      = local.uc_role_name
}

resource "aws_iam_role" "uc_access" {
  count = var.create_storage ? 1 : 0

  name               = local.uc_role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.e2e[0].json
}

resource "aws_iam_role_policy" "uc_access" {
  count = var.create_storage ? 1 : 0

  name   = "s3-access"
  role   = aws_iam_role.uc_access[0].id
  policy = data.databricks_aws_unity_catalog_policy.e2e[0].json
}

# --- Databricks side ---

# Created BEFORE the IAM role exists (see the chicken-and-egg note above), so
# validation must be skipped at create time; the external location below
# validates the whole chain once the role is in place.
resource "databricks_storage_credential" "e2e" {
  count = var.create_storage ? 1 : 0

  name    = local.uc_role_name
  comment = "Redpanda Connect iceberg e2e (disposable)"
  aws_iam_role {
    # Constructed string on purpose — referencing aws_iam_role.uc_access here
    # would create a dependency cycle.
    role_arn = "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:role/${local.uc_role_name}"
  }
  skip_validation = true
  force_destroy   = true
}

# IAM is eventually consistent: creating the external location immediately
# after the role/policy routinely fails validation with an assume-role error.
# Same workaround the provider docs use for the cross-account workspace role
# (docs/guides/aws-workspace.md). If a live apply still trips on propagation,
# just re-run `terraform apply` — everything here is idempotent.
resource "time_sleep" "uc_role_propagation" {
  count = var.create_storage ? 1 : 0

  create_duration = "30s"
  depends_on = [
    aws_iam_role.uc_access,
    aws_iam_role_policy.uc_access,
  ]
}

resource "databricks_external_location" "e2e" {
  count = var.create_storage ? 1 : 0

  name            = "${var.prefix}-databricks-e2e-${random_id.suffix.hex}"
  url             = "s3://${aws_s3_bucket.e2e[0].bucket}/e2e"
  credential_name = databricks_storage_credential.e2e[0].id
  comment         = "Redpanda Connect iceberg e2e (disposable)"
  force_destroy   = true

  depends_on = [time_sleep.uc_role_propagation]
}

# No explicit grant is needed for the catalog to use this location as its
# storage_root: terraform's principal creates (and therefore owns) the
# external location, and UC owners hold all privileges on their securables,
# including CREATE MANAGED STORAGE.
