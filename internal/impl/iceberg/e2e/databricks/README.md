# Databricks Unity Catalog e2e — iceberg copy-on-write

Validates the iceberg output's `merge_strategy: copy-on-write` against a real
Databricks Unity Catalog: writes go through the UC Iceberg REST endpoint
(`https://<host>/api/2.1/unity-catalog/iceberg-rest`), reads come back through
a serverless SQL warehouse via the SQL Statement Execution API.

## Prerequisites

- A Databricks workspace on **Premium or above** with **serverless SQL
  warehouses enabled**, attached to a Unity Catalog metastore.
- **Metastore external access** must be on for the Iceberg REST endpoint to
  work. Flipping it needs a **METASTORE ADMIN** (usually a one-time manual
  action; ask your account/metastore admin if that isn't you):

  ```sh
  databricks metastores summary                      # note the metastore id
  databricks metastores update <metastore-id> --json '{"external_access_enabled": true}'
  ```

  Alternatively set `manage_external_access = true` (plus `metastore_id`) and
  terraform runs that CLI call for you.
- **Storage root check**: if `databricks metastores summary` shows no default
  storage root (common on auto-provisioned metastores), catalog creation needs
  an explicit one — set `TF_VAR_storage_root=s3://bucket/prefix` (or the
  `storage_root` variable) before applying.

## Auth

Quick-start with a personal access token (User Settings → Developer → Access
tokens in the workspace UI, or `databricks tokens create`):

```sh
export DATABRICKS_HOST="https://dbc-abc123.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export TF_VAR_workspace_host="$DATABRICKS_HOST"
```

Both terraform and the tests read the token from `DATABRICKS_TOKEN` only — it
is never a terraform variable/output, taskfile var, or test flag, so it can't
end up in state or logs.

OAuth2 (M2M service principal) also works for terraform, but the tests use the
PAT bearer token for the Iceberg REST client deliberately: community reports
intermittent 500s using OAuth2 tokens against the UC IRC endpoint.

## Running

```sh
task terraform:apply   # catalog + schema + serverless warehouse + grants
task test              # the e2e suite (skips itself if unconfigured)
task terraform:destroy # tear everything down
```

`task bench` runs the commit-latency measurements; `task full` chains
apply → test → destroy (destroy is deferred, so it still runs when tests
fail — but if apply itself dies partway, run `task terraform:destroy`
manually).

Tests use unique per-run table names and drop their tables via
`DROP TABLE IF EXISTS` on cleanup, so repeated `task test` runs don't need a
terraform re-apply.

## Cost

Minimal: one 2X-Small serverless SQL warehouse with `auto_stop_mins = 1`
(statement submission auto-restarts it), and a few thousand tiny rows at most
(the bench writes ~20k rows total). Destroy when done and nothing keeps
billing.

## Permission asterisks (and fallbacks)

1. **`EXTERNAL USE SCHEMA`** is not part of `ALL PRIVILEGES` and only the
   *catalog owner* can grant it. Terraform's principal creates the catalog and
   so owns it, which is why the self-grant in `main.tf` should work. Fallback
   if the grant fails: have the catalog owner run
   `GRANT EXTERNAL USE SCHEMA ON CATALOG <catalog> TO <principal>` in a SQL
   editor.
2. **`external_access_enabled`** on the metastore needs METASTORE ADMIN.
   Fallback: the one-line CLI call above, run once by an admin — after that
   `manage_external_access` can stay `false` forever.

## Unverified until the first live run

Written before live credentials existed; verify these on first contact:

- the `databricks_grants` privilege string `EXTERNAL_USE_SCHEMA` (and whether
  UC tolerates the redundant self-grant to the owner) — `terraform/main.tf`;
- the `null_resource` local-exec `databricks metastores update` invocation —
  `terraform/main.tf`;
- the UC Iceberg REST behaviours the tests probe (CREATE TABLE acceptance,
  identifier-field-ids rejection wording, set-properties commits for the
  timestamp-encoding pin, equality-delete commit handling) — `e2e_test.go`.
