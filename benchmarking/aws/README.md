# AWS Benchmarking Framework

Production-shaped benchmarks and soak tests for Redpanda Connect connectors,
run on real AWS infrastructure in a dedicated, disposable account.

This tree contains the framework core plus the **postgres_cdc** and
**mysql_cdc** stacks — the subset needed by the soak pipeline (CON-179 R6).
Further connector stacks (sqlserver, oracle, mongodb, dynamodb, iceberg)
exist on the original development fork and land here with their own PRs,
each bringing its scenarios and tests.

## What a run does

One command turns a scenario YAML into:

1. An AWS environment via Terraform — VPC, runner EC2, load-gen EC2, a
   3-broker Redpanda cluster, the source (RDS Postgres), a results bucket.
2. A `redpanda-connect` binary built from your working tree, staged to the
   runner host (or, for `/soak` A/Bs, pre-built binaries via `--binary`).
3. A seeded dataset and a sustained write workload against the real source,
   captured through the real replication path (logical replication here).
4. Measurement: broker-side throughput (ground truth), Connect's own
   rolling stats, /metrics scrapes (goroutines, RSS, GC), and — for soak
   runs — per-minute CloudWatch emission and a derived backlog series.
5. Results: JSON + markdown locally; for soak runs, durable archive to
   `redpanda-connect-bench-soak-archive` and a soak-index entry.
6. Automatic teardown, with the orphan-reaper Lambda as the backstop.

Two run profiles share all of this:

- **Bench** (`scenarios/postgres/orders-cdc.yaml`): short, maximum-load CPU
  sweep to find ceilings.
- **Soak** (`orders-soak.yaml`, `orders-soak-pr.yaml`, `soak: true`):
  long, moderate-load (~10–15% of ceiling) endurance runs that catch leaks,
  stalls, and rotation bugs. Operations guide: **[SOAK.md](SOAK.md)**.

## Scheduled + PR-triggered runs

- `.github/workflows/soak_nightly.yml` — nightly soak (08:10 UTC, cron
  active on the default branch), **change-gated**: skips when nothing
  relevant merged since the last soaked commit. Manual dispatch always runs.
- `.github/workflows/soak_pr.yml` — comment `/soak` on a PR (write access
  required) for a base-vs-PR comparison on identical infra, posted back as
  a sticky comment.

Both authenticate via GitHub OIDC (no stored keys) and fetch the enterprise
license from Secrets Manager.

## Running from a laptop

```bash
# one-time (per account): persistent stack (dashboards, alarms, reaper,
# OIDC, archive bucket) + license secret — see SOAK.md
cd benchmarking/aws && task aws:persistent   # builds cleanup-lambda/bootstrap.zip itself

# validate a scenario (no AWS spend)
task aws:validate scenario=postgres/orders-cdc

# run a bench (~25 min infra + sweep; ~$2-3)
aws-vault exec <profile> -- env REDPANDA_LICENSE_FILEPATH=<path> \
  task aws:bench scenario=postgres/orders-cdc

# tear down after a failed/kept run
task aws:down scenario=postgres/orders-cdc
```

Hard-won operational rules (each cost a real incident — details in SOAK.md):

- **One bench at a time.** All sessions share one Terraform state and
  stack; the runner's pre-flight refuses to start while bench EC2 exists.
- **Credentials must outlive the run.** `aws-vault exec` static creds die
  at ~1h; prefer the workflows, or an SSO-session profile.
- **Ctrl-C once, never twice.** Repeated interrupts kill the deferred
  teardown mid-destroy.
- **Log lines are not teardown proof.** Verify with EC2/RDS queries; the
  workflows do this automatically.
- Run from a git worktree if you might switch branches before teardown.

## Orphan cleanup

`cleanup-lambda/` (deployed by the persistent stack, every 15 min) destroys
any `Project=redpanda-connect-bench` resource older than 4h. It lives
deliberately OUTSIDE the session stacks — a safety net must not share a
lifecycle with what it guards. A legitimate >4h run needs the rule disabled
first (`aws events disable-rule --name redpanda-connect-bench-orphan-cleanup`),
and re-enabled after.

## End-to-end sinks: Iceberg

An e2e run with an Iceberg sink writes through the Glue Iceberg REST catalog
into a scratch Glue database, `bench_<session>`, with table data under
`iceberg/<session>/` in the session results bucket. The runner deletes the
database (tables first) at teardown, even when the sweep fails.

- The reaper deletes any `bench_*` Glue database older than
  `orphan_ttl_hours` (4 h). E2E iceberg runs of about 2 h fit inside that;
  a longer custom run needs the reaper rule disabled, as for soaks (see
  Orphan cleanup).
- The orchestrator creates and deletes the Glue database with its own
  credentials: the GHA provisioner role (AdministratorAccess) or admin laptop
  credentials. The runner host role gets only `bench_*`-scoped Glue grants.
- If the account's Glue Data Catalog is governed by Lake Formation, the runner
  host role also needs Lake Formation grants on `bench_*`. The operator must
  check this on the first run in an account.

## End-to-end sinks: Snowflake

An e2e run with a Snowflake sink writes to a scratch schema,
`BENCH_<SESSION>`, in the bench Snowflake account. The runner resizes the
bench warehouse to the scenario's `sink.size` (default `XSMALL`) for the run.
At teardown, even when the sweep fails, it drops the schema with `CASCADE` and
sets the warehouse back to `XSMALL`.

Bench Bot plans pin the warehouse at `XSMALL` for both `snowflake_streaming`
and `snowflake_put`, and never offer a size: neither path runs `COPY` on the
warehouse in this harness (Snowpipe Streaming is serverless, and
`snowflake_put` only stages files), so the size does not change the
measurement. The catalogue entry records the size in `infra.sink_size`.

- Credentials are in the Secrets Manager secret
  `redpanda-connect-bench/snowflake`, as JSON
  `{account, user, private_key, database, warehouse, role}`. The operator
  creates it by hand; Terraform does not. Until it exists, the reaper skips its
  Snowflake sweep.
- The orchestrator reads the secret with its own credentials for its SQL
  (provision, count, teardown). Connect gets the key on the runner host only.
  Each point's bench script fetches the secret, writes `private_key` to
  `/opt/bench/secrets/snowflake_key.p8` (mode 0600), and removes the file on
  exit. The output reads the file with `private_key_file`. The key is never in
  a placeholder, a rendered config, an SSM command or a log line.
- The runner host role can read the secret only during a run that needs it.
  Its grant exists only when the shared stack is applied with
  `sink_secrets_enabled=true`, and the runner sets that only for e2e
  scenarios with a Snowflake sink. Soaks, which may run PR binaries, never
  get it. E2E scenarios refuse `--binary` mappings: bot runs build from the
  checkout only.
- The output creates its table, `BENCH_<SESSION>.T`, through schema
  evolution. The plan forces `schema_evolution.enabled: true` and keeps the
  customer's other `schema_evolution` settings. When the customer had it off
  or unset, the plan carries the caveat `schema_evolution_forced`. Until the
  first write the table does not exist, so it counts as 0. A missing table
  after rows have been counted fails the run.
- The count check reads `SELECT COUNT(*)` before and after each point. Snowpipe
  Streaming rows become visible with a delay, so each count is re-read every
  10 s until two reads agree, for at most 90 s. `snowflake_put` loads no rows
  (the customer's Snowpipe is not kept), so its count check is skipped.
- The reaper drops any `BENCH_*` schema whose `created_on` is older than
  `orphan_ttl_hours` (4 h). Do not create `BENCH_*` schemas by hand in the
  bench database: the reaper drops them.
- When no `BENCH_*` schema younger than the TTL remains, the reaper also sets
  the warehouse back to `XSMALL`, in case a crashed run left it resized. A
  live run re-applies its size immediately before each point, so this reset
  cannot make a point run at the wrong size.
- The bench warehouse must have `AUTO_SUSPEND` set (for example 60 s), so a
  warehouse that a crashed run left oversized bills for at most that long
  after its last query, not until the reaper runs.
- **Single lane until part 3.** Concurrent runs share the one bench warehouse,
  so one run's resize (and its teardown reset to `XSMALL`) changes the other's
  reference size. Run one Snowflake e2e scenario at a time.

## End-to-end sinks: BigQuery

An e2e run with a BigQuery sink writes to a scratch dataset,
`bench_<session>`, in the bench GCP project. At teardown, even when the sweep
fails, it deletes the dataset with its contents (already gone counts as done).

- Credentials are in the Secrets Manager secret
  `redpanda-connect-bench/bigquery`, as JSON
  `{project, credentials_json, location}`. `credentials_json` is a
  service-account key JSON string; `location` is optional and defaults to
  `US`. The operator creates the secret by hand; Terraform does not. Until it
  exists, the reaper skips its BigQuery sweep.
- The service account needs BigQuery Data Editor and BigQuery Job User on the
  bench project only. Do not create `bench_*` datasets by hand there: the
  reaper deletes any `bench_*` dataset older than `orphan_ttl_hours` (4 h).
- The orchestrator reads the secret with its own credentials for its API calls
  (create, count, delete). Connect gets the key on the runner host only. Each
  point's bench script fetches the secret, writes `credentials_json` to
  `/opt/bench/secrets/bigquery.json` (mode 0600), and removes the file on exit.
  `gcp_bigquery` has no credentials-file field, so Connect is launched with
  `GOOGLE_APPLICATION_CREDENTIALS=/opt/bench/secrets/bigquery.json` and uses
  application-default credentials; the plan removes `credentials_json` from
  the config. The key is never in a placeholder, a rendered config, an SSM
  command or a log line.
- The runner host role can read the secret only during a run that needs it,
  under the same `sink_secrets_enabled` gate as the Snowflake secret.
- The output creates its table, `t`. The plan forces
  `create_disposition: CREATE_IF_NEEDED`; when the customer had `CREATE_NEVER`
  the plan carries the caveat `schema_evolution_forced`. Until the first write
  the table does not exist, so it counts as 0. A missing table after rows have
  been counted fails the run.
- The count check reads `SELECT COUNT(*)` before and after each point. Streamed
  rows become visible with a delay, so each count is re-read every 10 s until
  two reads agree, for at most 90 s.

## Results catalogue

Every completed bench sweep writes one entry to the results catalogue in the
`redpanda-connect-bench-catalog` bucket, at
`catalog/v1/<input>/<entry_id>.json`. Only bench sweeps are catalogued: soak
runs and arm and binary A/B runs are not, and the runner does not write
entries for builds from a dirty working tree (`catalog import` cannot detect a
dirty build, so check that before importing).
Each entry records the connector identity, the workload, the Connect version
and build SHA, and the measured capacity at each vCPU point. The sizing
constants (headroom, CUs per vCPU, bytes per CU) live in
`catalog/sizing.json`.

The `catalog` CLI (`go run ./cmd/catalog`) reads and fills it. It prints JSON.

```sh
# find entries matching a workload (or pass --config <pipeline.yaml>)
go run ./cmd/catalog query --repo ../.. --input postgres_cdc --output redpanda --message-size 1200

# size CUs for a target throughput from one entry
go run ./cmd/catalog recommend --repo ../.. --entry-id <entry_id> --target-mb-s 40

# backfill entries from historical result JSON
go run ./cmd/catalog import --repo ../.. --results-root <dir laid out as connector/scenario/*.json>
```

`--repo` must be a full clone of this repo with tags fetched
(`git fetch --tags`). Staleness compares each entry's build SHA against the
latest release tag, and `import` reads scenarios and describes build SHAs
from it, so a shallow or tagless clone gives wrong or `unknown` answers.

An e2e entry's `capacity_mb_s` is input-equivalent (`capacity_basis:
input_equivalent`): the msg/s measured at the output times the workload's
message size, so a pipeline that shrinks or grows messages is still sized by
the input rate it sustains. The bytes measured at the output are in each
point's `output_mb_s`. Source entries have no `capacity_basis`: their
capacity is the output bytes, as before.

The catalogue app on Google Cloud Run reads this bucket and creates plan
objects through the `redpanda-connect-bench-catalog-app` role, by GCP web
identity (no AWS keys). The persistent stack creates it only when
`bench_catalog_gcp_sa_unique_id` is set; read the id with
`gcloud iam service-accounts describe bench-catalog-sa@rp-internal-agents.iam.gserviceaccount.com --format='value(uniqueId)'`.
The role's ARN is the output `bench_catalog_app_role_arn`.

## Bench Bot runs by hand (operator)

The Bench Bot agent normally builds, claims and dispatches plans. To run one
by hand (for example the first end-to-end run of a new account):

```sh
# 1. build: store a plan from a customer config
go run ./cmd/benchplan build --config cfg.yaml --message-size 1024 --target-mb-s 40 \
  --store s3://redpanda-connect-bench-catalog
# 2. claim: the approval the workflow refuses to run without
go run ./cmd/benchplan claim --plan s3://redpanda-connect-bench-catalog/plans/<id>.json --approver <your name>
# 3. dispatch, only if claim printed "won": true
gh workflow run bench_bot.yml -f plan_id=<id>
```

The workflow's log is public: it prints `benchplan summary` (component names,
caveats, sink, estimate), never the plan's pipeline. `benchplan show` prints
the whole plan and is for local use only. Before taking the shared-stack lock
the workflow lints the rendered Connect config; a config Connect rejects ends
with outcome `invalid_config` and spends nothing.

## Layout

| Path | Role |
|---|---|
| `runner/` | Go orchestrator: provision → stage → seed → sweep/soak → results → teardown |
| `scenarios/<engine>/` | bench + soak + PR-comparison scenarios (postgres, mysql) |
| `seeders/cdc-rows-<engine>/` | write-workload generators |
| `terraform/shared/` | per-session VPC, hosts, brokers, results bucket |
| `terraform/stacks/<engine>/` | per-session RDS Postgres / RDS MySQL |
| `terraform/persistent/` | applied once: dashboards, alarms, OIDC, reaper, archive |
| `catalog/`, `cmd/catalog/` | results catalogue and its query/recommend/import CLI |
| `cleanup-lambda/` | the orphan reaper (own Go module) |
| `SOAK.md` | soak operations runbook |

## Known limitations

- postgres_cdc IAM auth cannot work against vanilla RDS (replication
  connections reject IAM tokens); the credential-rotation soak window
  belongs to the mysql_cdc soak, which runs password auth today — the IAM
  increment is still open (see SOAK.md).
- One-lane serialization: soaks and benches queue on the shared stack.
  Session-scoped isolation is the tracked scaling path.
- The weekly 24h soak needs a reaper exemption tag + a non-GitHub conductor
  (6h job cap) before it can exist.

## Cost

A postgres bench run: ~$2–3. A nightly soak: ~$5 (and $0 on change-gated
skip days). The stranded-stack worst case is bounded by the reaper's 4h TTL.
