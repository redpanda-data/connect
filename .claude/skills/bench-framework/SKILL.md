---
name: bench-framework
description: Use when working in benchmarking/aws/ on the Redpanda Connect benchmarking framework — adding a new connector bench (TF stack + scenario + engineSpec + kcConnectorSpec), running an existing bench, or debugging a bench failure. Covers both the Connect-vs-Kafka-Connect comparison framework and operational essentials (aws-vault, license placement, SIGINT teardown).
---

# Redpanda Connect Bench Framework

This skill helps engineers work with the AWS bench framework in `benchmarking/aws/`. The framework runs a Connect-vs-Kafka-Connect head-to-head sweep across vCPU points on the same hardware, with broker-side throughput as the canonical metric.

**Assumes:** Connect experience, bench-new. The skill explains bench concepts (sweep points, vCPU pinning, fairness) but does not re-teach Redpanda Connect itself.

## Pre-flight: traps to know before you start

Each line links to [references/traps.md](references/traps.md). Skim before any bench session — these are non-obvious and have all bitten in production.

| Trap | One-liner |
|------|-----------|
| 1 | Wrap `task aws:bench` in `aws-vault exec bench`, not `AWS_PROFILE=`. [link](references/traps.md#aws-vault) |
| 2 | Place the Connect Enterprise license at the repo root, not `~/Downloads/`. [link](references/traps.md#license-location) |
| 3 | Stop a stuck bench with SIGINT (Ctrl+C), never SIGKILL. [link](references/traps.md#sigint) |
| 4 | `defer destroy` is registered BEFORE the first `terraform apply`. Don't refactor that order. [link](references/traps.md#defer-destroy-before-apply) |
| 5 | RDS Postgres uses `rds.logical_replication`, not `wal_level`. [link](references/traps.md#rds-logical-replication) |
| 6 | `postgres_cdc` tls block has no `enabled:` toggle. [link](references/traps.md#postgres-cdc-tls) |
| 7 | `write_rate_per_sec` is per-table-per-second, not total. [link](references/traps.md#workload-rate-per-table) |
| 8 | TRUNCATE between sweep points required. [link](references/traps.md#truncate-between-points) |
| 9 | Redpanda's `auto_create_topics_enabled` defaults FALSE (opposite of Apache Kafka). [link](references/traps.md#redpanda-auto-create-topics) |
| 10 | Metric label is `redpanda_topic`, not `topic`. [link](references/traps.md#redpanda-topic-label) |
| 11 | Per-topic byte metrics are per-broker — scrape ALL brokers. [link](references/traps.md#per-broker-metrics) |
| 12 | Per-vCPU KC connector name for offset isolation: `bench_<conn>_v<N>`. [link](references/traps.md#kc-connector-offset-isolation) |
| 13 | MySQL Debezium needs `snapshot.mode=no_data`, not `never`. [link](references/traps.md#mysql-debezium-snapshot) |
| 14 | Don't use `chrt --fifo` with JVM — deadlocks under single-core taskset. [link](references/traps.md#chrt-fifo-deadlock) |
| 15 | Runner and load-gen need DIFFERENT cloud-init templates (KC split-brain). [link](references/traps.md#cloud-init-runner-loadgen) |
| 16 | Orphan-cleanup Lambda's 4h TTL can trip long benches. [link](references/traps.md#orphan-ttl) |
| 17 | `bench_session_id` is NOT a TF output — runner injects it post-apply. [link](references/traps.md#bench-session-id-not-output) |

## What are you doing?

**First time on this account?** Do the one-time setup in [references/bootstrap.md](references/bootstrap.md) before anything else (~30 min). Each engineer brings their own AWS account, S3 state bucket, DDB lock table, and license — the bootstrap recipe walks all of it.

1. **Adding a new connector bench** → [Add a new connector](#add-a-new-connector). Skill walks 9 steps to produce the TF stack, scenario YAML, engineSpec entry, kcConnectorSpec entry, reset SQL, plus tests.
2. **Running an existing bench** → [Operate a bench](#operate-a-bench). Skill walks the pre-flight checklist, the run, live monitoring, and teardown.
3. **Debugging a failed bench** → [Debug](#debug). Skill triages from symptom into the playbook.

## Add a new connector

> _Filled in by Phase D._

## Operate a bench

If you're running an existing bench (not adding a new connector), walk this checklist top-to-bottom. The pre-flight is mandatory; the rest is the actual run.

### Step 1: Pre-flight checklist

If you've never run this bench before, do the one-time setup in [references/bootstrap.md](references/bootstrap.md) first (~30 min). Otherwise:

- [ ] aws-vault profile set up (see [bootstrap.md Step 1](references/bootstrap.md))
- [ ] S3 state bucket + DDB lock table created (see [bootstrap.md Steps 2-3](references/bootstrap.md))
- [ ] `backend.hcl` + stacks' `main.tf` backend blocks edited to your bucket name (see [bootstrap.md Step 4](references/bootstrap.md))
- [ ] Redpanda Connect Enterprise license at repo root as `rpcn.license` (NOT `~/Downloads/`)
- [ ] On `benchmarking` branch with latest commits (`git pull origin benchmarking`)
- [ ] `make zip` run inside `benchmarking/aws/cleanup-lambda/` recently (orphan cleanup needs the artifact)

Full rationale: [references/workflow-essentials.md](references/workflow-essentials.md).

### Step 2: Pick the scenario

List existing scenarios:

```bash
ls benchmarking/aws/scenarios/*/*.yaml
```

Wall-clock and spend estimates:

| Scope | Wall-clock | Spend |
|-------|------------|-------|
| 1-vCPU smoke | ~25 min | ~$1.50 |
| 4-point sweep × 2 engines | ~2.5-3h | ~$8 |
| Full 4-point sweep × 2 engines at 150K writes/sec | ~3-4h | ~$10-12 |

Long benches (>4h) risk the orphan-cleanup TTL — see [traps.md#orphan-ttl](references/traps.md#orphan-ttl).

### Step 3: Validate (free, no AWS spend)

```bash
task aws:validate scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml
```

Catches YAML typos, missing engineSpec/kcConnectorSpec entries, sizing-trap candidates.

### Step 4: Run

```bash
aws-vault exec bench -- task aws:bench \
  scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml \
  [--engines connect,kafka_connect] \
  [--keep-on-fail]
```

- `--engines` defaults to `connect` only. Pass both to get the head-to-head sweep.
- KC roughly doubles wall-clock per vCPU point (sequential, same runner host).
- `--keep-on-fail` preserves infra for live debug — see [references/workflow-essentials.md#6](references/workflow-essentials.md).

### Step 5: Live monitoring

While the bench runs:

- Heartbeat lines every 60s show rolling stats. "Good" = steady ≥ 20 MB/s once warmup ends.
- S3 paths to check (replace `<sess>` with the session id printed at startup):
  - `s3://<bucket>/runs/<sess>/sweep-<vcpu>.log` — Connect's full output per point
  - `s3://<bucket>/runs/<sess>/prom-<vcpu>.txt` — `:4195/metrics` snapshots
  - `s3://<bucket>/runs/<sess>/redpanda-{connect,kc}-<vcpu>.txt` — broker `/public_metrics`
  - `s3://<bucket>/runs/<sess>/kc-<vcpu>.log` — KC JVM output

If you need to interrupt: **SIGINT (Ctrl+C) only**. SIGKILL strands infra. See [traps.md#sigint](references/traps.md#sigint).

### Step 6: Teardown

Happy path: bench finishes, `defer destroy` fires automatically.

Hung-bench path:

```bash
aws-vault exec bench -- task aws:down
```

If `task aws:down` fails with `bench_session_id: required variable not set`, see [debugging-playbook.md#tf-destroy-session-id](references/debugging-playbook.md#tf-destroy-session-id).

### Step 7: Inspect results

```bash
ls benchmarking/aws/results/<connector>/<scenario>/
```

Each run produces a JSON + adjacent markdown. The auto-refreshed `benchmarking/aws/SUMMARY.md` table aggregates the latest run per scenario.

How to read the per-run markdown:

| Column | Meaning |
|--------|---------|
| `engine` | `connect` or `kafka_connect` |
| `MB/sec (p50)` | Median throughput from rolling stats |
| `broker MB/s` | Broker-derived median (canonical fairness metric) |
| `MB/sec (p5)` / `(p95)` | Tail spread |
| `msg/sec (p50)` | Median msg rate |
| `Δ vs Connect` | Diff vs Connect at same vCPU (only on the KC row) |
| `⚠` flag on SUMMARY row | Cross-engine divergence > 2× detected |

### Step 8: On failure

Go to [Debug](#debug).

## Debug

Look up the symptom in the table below, then read the linked playbook entry. Each entry ends with the existing fix's commit SHA.

| Symptom | Likely cause | Playbook |
|---------|--------------|----------|
| Connect `MedianMBPerSec=0`, no `[load]` lines | Workload generator silent | [link](references/debugging-playbook.md#empty-workload) |
| KC HTTP 500 with no body | `curl --fail` suppressed body | [link](references/debugging-playbook.md#kc-http-500) |
| `broker_series` empty though KC log shows writes | Wrong label name OR single-broker scrape | [link](references/debugging-playbook.md#broker-series-empty) |
| `UNKNOWN_TOPIC_OR_PARTITION` errors | Redpanda `auto_create_topics_enabled=false` | [link](references/debugging-playbook.md#auto-create) |
| KC at vCPU≥2 reports 0 MB/s, log says `redo log is no longer available` | Debezium offset persistence | [link](references/debugging-playbook.md#offset-isolation) |
| `could not translate host name` RDS endpoint | VPC DNS flake | [link](references/debugging-playbook.md#dns-flake) |
| Bench hangs after several hours, SSM "InstanceId not found" | Orphan-cleanup Lambda fired | [link](references/debugging-playbook.md#orphan-ttl) |
| `task aws:down` fails: `bench_session_id required` | Shared stack declares it required | [link](references/debugging-playbook.md#tf-destroy-session-id) |
| `terraform init shared: exit status 1` "Backend configuration changed" | Stale `.terraform/` dirs | [link](references/debugging-playbook.md#terraform-init-backend-changed) |
| `ConditionalCheckFailedException: Error acquiring the state lock` | Stale DDB lock or missing table | [link](references/debugging-playbook.md#rds-dynamodb-lock) |

If the symptom isn't here, search [traps.md](references/traps.md) by keyword and check git log for recent fixes on the `benchmarking` branch.
