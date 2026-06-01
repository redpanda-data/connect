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

> _Filled in by Phase B._

## Debug

> _Filled in by Phase C._
