terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
    # aws + time are only exercised when create_storage = true (see storage.tf).
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }
  # >= 1.9 for the cross-variable validation on create_storage in variables.tf.
  required_version = ">= 1.9"
}

# Authentication comes exclusively from the environment: DATABRICKS_HOST and
# DATABRICKS_TOKEN. The token is deliberately NOT a terraform variable so it
# can never appear in plan output, outputs, or state.
provider "databricks" {}

# Only used when create_storage = true (variable validation enforces that
# aws_region is set in that case). Terraform configures every declared
# provider even when all of its resources have count = 0, and the AWS
# provider hard-fails configuration when its credential chain resolves
# nothing — which would break the default create_storage = false path on
# machines with no AWS setup at all. So when the AWS side is inactive this
# block pins placeholder static credentials + region and skips every
# configure-time check (no AWS API call can ever happen: all aws_* resources
# have count = 0). When create_storage = true the placeholders are null and
# credentials come from the usual environment/shared-config chain.
provider "aws" {
  region     = var.aws_region != "" ? var.aws_region : "us-east-1"
  access_key = var.create_storage ? null : "mock-unused-access-key"
  secret_key = var.create_storage ? null : "mock-unused-secret-key"

  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
}

data "databricks_current_user" "me" {}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  # Bare workspace host (no scheme, no trailing slash), whatever form the
  # workspace_host variable arrived in.
  workspace_host = trimsuffix(trimprefix(trimprefix(var.workspace_host, "https://"), "http://"), "/")
  catalog_name   = "${var.prefix}_e2e_${random_id.suffix.hex}"

  # Managed storage root for the e2e catalog. Precedence:
  #   1. an explicit storage_root variable always wins;
  #   2. else, when create_storage = true, the external location provisioned
  #      in storage.tf (a per-apply subpath so re-created catalogs never
  #      collide on a previously-used root);
  #   3. else null — inherit the metastore's default storage root.
  catalog_storage_root = (
    var.storage_root != "" ? var.storage_root :
    var.create_storage ? "${databricks_external_location.e2e[0].url}/${local.catalog_name}" :
    null
  )
}

# --- Isolated Unity Catalog environment ---

resource "databricks_catalog" "e2e" {
  name          = local.catalog_name
  comment       = "Redpanda Connect iceberg copy-on-write e2e (disposable)"
  force_destroy = true # cascades: schemas and test tables go with the catalog

  # storage_root is REQUIRED when the metastore has no default storage root
  # (common on auto-provisioned metastores). Check with:
  #   databricks metastores summary
  # and either set the storage_root variable to e.g. s3://bucket/prefix, or
  # set create_storage = true to have this config provision a bucket +
  # external location itself (see storage.tf and local.catalog_storage_root).
  storage_root = local.catalog_storage_root
}

# The Iceberg namespace used by the tests. Pre-created here because
# client-side namespace creation through UC's Iceberg REST endpoint is
# unverified — the tests never call CreateNamespace.
resource "databricks_schema" "e2e" {
  catalog_name  = databricks_catalog.e2e.name
  name          = var.schema_name
  comment       = "Namespace for Redpanda Connect iceberg e2e tables"
  force_destroy = true
}

# Serverless SQL warehouse for reading written data back via the SQL
# Statement Execution API. 2X-Small, single cluster, auto-stops after a
# minute idle — statement submission auto-restarts it, so cost stays minimal.
resource "databricks_sql_endpoint" "e2e" {
  name                      = "${var.prefix}-e2e-${random_id.suffix.hex}"
  cluster_size              = "2X-Small"
  min_num_clusters          = 1
  max_num_clusters          = 1
  auto_stop_mins            = 1
  enable_serverless_compute = true
  warehouse_type            = "PRO"
}

# --- Grants ---
#
# The Iceberg REST principal needs EXTERNAL USE SCHEMA on top of the usual
# privileges. EXTERNAL USE SCHEMA is NOT included in ALL PRIVILEGES and only
# the catalog owner can grant it — terraform's principal creates the catalog
# and is therefore its owner, so granting itself here should work.
#
# UNVERIFIED-WITHOUT-LIVE-ACCESS: the exact provider privilege string for
# EXTERNAL USE SCHEMA ("EXTERNAL_USE_SCHEMA") and whether UC allows a
# redundant self-grant to the owner must be confirmed on the first live run.
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.e2e.name

  grant {
    principal = data.databricks_current_user.me.user_name
    privileges = [
      "USE_CATALOG",
      "USE_SCHEMA",
      "CREATE_TABLE",
      "MODIFY",
      "SELECT",
      "EXTERNAL_USE_SCHEMA",
    ]
  }
}

# --- Metastore external access (OPTIONAL, off by default) ---
#
# The Iceberg REST endpoint only works when the metastore has
# external_access_enabled = true. Flipping it needs METASTORE ADMIN, so this
# is usually a one-time manual/admin action:
#
#   databricks metastores update <metastore-id> --json '{"external_access_enabled": true}'
#
# Set manage_external_access = true (plus metastore_id) to have terraform run
# that CLI call for you. Implemented as a null_resource local-exec because
# adopting the whole metastore into state via the databricks_metastore
# resource (which does expose external_access_enabled) would be far more
# invasive than a disposable e2e environment warrants.
#
# UNVERIFIED-WITHOUT-LIVE-ACCESS: the CLI invocation below is written per the
# official docs and needs confirming on the first live run. Note it is not
# reverted on destroy.
resource "null_resource" "enable_external_access" {
  count = var.manage_external_access ? 1 : 0

  triggers = {
    metastore_id = var.metastore_id
  }

  lifecycle {
    precondition {
      condition     = var.metastore_id != ""
      error_message = "metastore_id must be set when manage_external_access = true (find it with: databricks metastores summary)."
    }
  }

  provisioner "local-exec" {
    command = "databricks metastores update ${var.metastore_id} --json '{\"external_access_enabled\": true}'"
  }
}
