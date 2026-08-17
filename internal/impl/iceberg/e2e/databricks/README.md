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
  an explicit one — either set `TF_VAR_storage_root=s3://bucket/prefix` (or
  the `storage_root` variable) before applying, or set `create_storage=true`
  to have terraform provision a bucket + external location itself (see the
  trial quick-start below; works for company accounts too). An explicit
  `storage_root` always wins over `create_storage`.

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

## Trial account quick-start (no company workspace needed)

A Databricks 14-day express trial can run this whole suite — with one twist:
trial workspaces use [default storage](https://docs.databricks.com/aws/en/storage/default-storage),
which does **not** support credential vending for external clients ("such as
when external systems connect to the Unity REST API or Iceberg REST catalog",
per that doc). A catalog on default storage therefore **cannot** work for
these tests, no matter the grants. Serverless trial workspaces *do* support
catalogs on customer-owned S3, which is exactly what `create_storage=true`
provisions (bucket + IAM role + UC storage credential + external location,
all disposable). You need an AWS account for the bucket; the ~$400 trial
credit covers the Databricks side.

Known trial constraints:

- **Free Edition cannot work at all** — no external data access. Use the
  trial from [databricks.com/try-databricks](https://www.databricks.com/try-databricks).
- Sign up with a **business email** (no card needed, ~$400 of credits over
  14 days). Personal-email trials are capped at a single SQL warehouse, which
  bites the moment anything else holds one — business email avoids that.
- Trial workspace assets are deleted **60 days after the trial expires** —
  nothing here is worth keeping anyway, but don't park anything you love in
  it.

Steps:

1. Sign up, open the workspace, and grab a PAT (User Settings → Developer →
   Access tokens).
2. Enable external data access on the metastore (as the trial's only user you
   are the account admin; if the call is refused, make yourself metastore
   admin first in the account console under Catalog → your metastore):

   ```sh
   databricks metastores summary                      # note the metastore id
   databricks metastores update <metastore-id> --json '{"external_access_enabled": true}'
   ```

3. Export Databricks and AWS credentials, plus the storage variables — use
   `TF_VAR_*` env vars (not one-off `-var` flags) so `terraform destroy` later
   sees the same values:

   ```sh
   export DATABRICKS_HOST="https://dbc-abc123.cloud.databricks.com"
   export DATABRICKS_TOKEN="dapi..."
   export TF_VAR_workspace_host="$DATABRICKS_HOST"
   export AWS_PROFILE=...                             # or AWS_ACCESS_KEY_ID etc.
   export TF_VAR_create_storage=true
   export TF_VAR_aws_region=us-east-1                 # bucket region
   ```

4. Apply and test as usual:

   ```sh
   task terraform:apply
   task test
   ```

   (`task terraform:apply -- -var create_storage=true -var aws_region=us-east-1`
   also works — args after `--` pass through — but then destroy needs the
   same flags, hence the env-var recommendation.)

The company-account path is unchanged: `create_storage` defaults to `false`
and nothing AWS-side is touched.

**If the first apply fails at the external location**: Unity Catalog storage
credentials have a chicken-and-egg with the IAM role (the role's trust policy
needs the credential's external ID), handled with the databricks provider's
documented pattern plus a 30s wait for IAM propagation. IAM is eventually
consistent, so a slow region can still occasionally fail the external
location's validation on the first try — just re-run `task terraform:apply`;
it picks up where it left off.

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

## Live-run status

The harness has run green end-to-end against a live Unity Catalog (serverless
workspace, customer-owned S3 via `create_storage = true`): the terraform chain
(catalog, schema, warehouse with 1-minute auto-stop, `EXTERNAL_USE_SCHEMA`
self-grant, storage credential and external location), all four tests, and the
commit-latency bench. Results are recorded in
`docs/benchmark-results/iceberg.md`.

Still unexercised — verify on first use:

- the `manage_external_access` `null_resource` (`databricks metastores
  update`) — the toggle was flipped manually via the CLI before terraform ran;
- OAuth M2M auth for the Iceberg REST client (PAT was used throughout);
- the explicit `storage_root` variable and metastore-root-inherit variants
  (only `create_storage = true` has been run).
