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
