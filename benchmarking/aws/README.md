# AWS Benchmarking Framework

Production-shaped benchmarks and soak tests for Redpanda Connect connectors,
run on real AWS infrastructure in a dedicated, disposable account.

This tree contains the framework core plus the **postgres_cdc** stack — the
subset needed by the soak pipeline (CON-179 R6). Further connector stacks
(mysql, sqlserver, oracle, mongodb, dynamodb, iceberg) exist on the original
development fork and land here with their own PRs, each bringing its
scenarios and tests.

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
cd benchmarking/aws && make -C cleanup-lambda zip && task aws:persistent

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

## Layout

| Path | Role |
|---|---|
| `runner/` | Go orchestrator: provision → stage → seed → sweep/soak → results → teardown |
| `scenarios/postgres/` | bench + soak + PR-comparison scenarios |
| `seeders/cdc-rows-postgres/` | write-workload generator |
| `terraform/shared/` | per-session VPC, hosts, brokers, results bucket |
| `terraform/stacks/postgres/` | per-session RDS Postgres |
| `terraform/persistent/` | applied once: dashboards, alarms, OIDC, reaper, archive |
| `cleanup-lambda/` | the orphan reaper (own Go module) |
| `SOAK.md` | soak operations runbook |

## Known limitations

- postgres_cdc IAM auth cannot work against vanilla RDS (replication
  connections reject IAM tokens); the credential-rotation soak window is
  covered by mysql_cdc when its stack lands.
- One-lane serialization: soaks and benches queue on the shared stack.
  Session-scoped isolation is the tracked scaling path.
- The weekly 24h soak needs a reaper exemption tag + a non-GitHub conductor
  (6h job cap) before it can exist.

## Cost

A postgres bench run: ~$2–3. A nightly soak: ~$5 (and $0 on change-gated
skip days). The stranded-stack worst case is bounded by the reaper's 4h TTL.
