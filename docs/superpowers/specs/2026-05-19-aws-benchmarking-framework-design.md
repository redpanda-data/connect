# AWS Benchmarking Framework — Design

**Date:** 2026-05-19
**Branch:** `benchmarking`
**Author:** prakhar.garg@redpanda.com

---

## Problem

Redpanda Connect today has per-connector benchmark suites in
`internal/impl/<category>/bench/` that run locally in Docker, drive customer-shaped
workloads against a `benchmark` processor, and emit `msg/sec` and `MB/sec` rolling
stats. Results live in `docs/benchmark-results/<connector>.md`. Variation across
CPU counts is done ad-hoc by exporting `GOMAXPROCS` or pinning Docker `cpuset`.

This gets us reasonable engineering numbers on a laptop, but customers run on
AWS. We need benchmark numbers that:

1. Reflect realistic customer workloads, not just saturation runs.
2. Are produced on real AWS infrastructure (not laptops / Docker).
3. Cover a consistent CPU sweep (`1, 2, 4, 8` vCPU) so we can speak to scaling.
4. Are absorbed against AWS network/throttle noise — long enough windows that a
   60s blip doesn't dominate the headline number.
5. Are reproducible and bounded in cost — operator runs one command, infra
   stands up, the matrix runs, results land in `docs/benchmark-results/`, infra
   is torn down.

## Goals & Non-Goals

**Goals**

- A `benchmarking/aws/` framework that turns a scenario YAML file into a fully
  automated end-to-end run: provision → seed → CPU sweep → render results → tear
  down.
- 13 shipped scenarios across 6 connectors (postgres, mysql, sqlserver,
  dynamodb, s3, iceberg) — two to three per connector, each shaped after a
  named customer story.
- MB/sec as the primary throughput metric, with `p5 / p50 / p95 / peak`
  reported per CPU point so reviewers can see variability.
- An onboarding path for new connectors that requires no Go changes to the
  runner — only a Terraform stack + scenario YAML.

**Non-goals**

- CI integration (scheduled or workflow-dispatch) — operator-driven only in
  this iteration.
- Oracle, migrator, and Salesforce connectors — explicitly out of scope.
- Buffers / caches / rate-limit benchmarks — only inputs/outputs.
- Cross-cloud (Azure, GCP) — AWS only.
- Customer-supplied data — synthetic generators only.

---

## Architecture

Three layers under a new top-level `benchmarking/aws/` directory:

```
benchmarking/aws/
├── README.md
├── Taskfile.yml                   # the operator's UX (one command)
├── terraform/
│   ├── backend.hcl                # shared S3 + DynamoDB-lock backend config
│   ├── shared/                    # VPC, SGs, runner EC2, load-gen EC2, IAM, results bucket
│   ├── modules/
│   │   ├── rds-postgres/
│   │   ├── rds-mysql/
│   │   ├── rds-sqlserver/
│   │   ├── dynamodb-tables/
│   │   ├── s3-bucket/
│   │   ├── msk-cluster/
│   │   └── glue-iceberg/
│   └── stacks/
│       ├── postgres/              # uses shared + rds-postgres
│       ├── mysql/                 # uses shared + rds-mysql
│       ├── sqlserver/             # uses shared + rds-sqlserver
│       ├── dynamodb/              # uses shared + dynamodb-tables
│       ├── s3/                    # uses shared + s3-bucket
│       ├── iceberg/               # uses shared + s3-bucket + glue-iceberg
│       └── kafka-to-pg/           # uses shared + rds-postgres + msk-cluster
├── scenarios/
│   ├── postgres/{orders-cdc,snapshot-large,kafka-to-pg}.yaml
│   ├── mysql/{orders-cdc,snapshot-large}.yaml
│   ├── sqlserver/{orders-cdc,multi-table-cdc}.yaml
│   ├── dynamodb/{orders-cdc,fanout-cdc}.yaml
│   ├── s3/{archive-write,batch-read}.yaml
│   └── iceberg/{events-append,large-batch-write}.yaml
├── seeders/                       # tiny Go programs for data generation
│   ├── cdc-rows/                  # shared across pg/mysql/sqlserver/dynamodb
│   ├── s3-objects/
│   └── iceberg-records/
├── runner/                        # Go orchestrator binary
│   ├── main.go                    # `runner bench <scenario>` entry point
│   ├── scenario.go                # parse + validate YAML
│   ├── terraform.go               # apply/destroy + var translation
│   ├── ssm.go                     # remote exec on runner / load-gen EC2 via SSM
│   ├── matrix.go                  # CPU sweep loop (cpuset + GOMAXPROCS)
│   ├── stats.go                   # parse benchmark-processor stdout, compute p5/p50/p95
│   ├── anomalies.go               # detect ≥60s dips below 0.8× median
│   └── render.go                  # write JSON + append markdown
└── results/                       # JSON per run (git-ignored; .gitkeep only)
```

Local Docker benches under `internal/impl/<x>/bench/` are unchanged. The AWS
framework is additive.

### The one command

```bash
task aws:bench -- scenario=postgres/orders-cdc
```

drives the full lifecycle: `terraform apply` (shared + per-connector stack) →
upload binary + configs → seed dataset → CPU sweep `[1, 2, 4, 8]` → write JSON +
append markdown → `terraform destroy`.

Convenience variants:

```bash
# Multiple scenarios on shared infra in one session
task aws:bench -- scenario=postgres/orders-cdc,postgres/snapshot-large

# Keep infra up between runs (iteration mode)
task aws:bench -- scenario=postgres/orders-cdc keep=true
task aws:down

# Keep infra on failure for debugging (default is destroy-even-on-failure)
task aws:bench -- scenario=postgres/orders-cdc keep-on-fail=true

# Validate without spending money (terraform plan + lint rendered pipeline)
task aws:validate -- scenario=postgres/orders-cdc
```

A run with shared infra reused across all scenarios for one connector takes
roughly `(provision ~6m) + (seed ~10m) + (Σ sweeps) + (destroy ~3m)`. Each sweep
is `4 × (2m warmup + 15m duration) ≈ 70 min`, so a single-scenario run is
~90 min total; a three-scenario session sharing infra is ~5 hours.

---

## Scenario format

A scenario is one YAML file. It captures the customer story, infra sizing, and
the matrix. Full example:

```yaml
# benchmarking/aws/scenarios/postgres/orders-cdc.yaml
name: postgres-orders-cdc
description: |
  Stream changes from a 75M-row orders table at 5K writes/sec sustained.
  Mirrors a mid-size e-commerce CDC pipeline (Postgres → drop, measuring
  read ceiling).

connector: postgres_cdc
stack: postgres                       # → benchmarking/aws/terraform/stacks/postgres

infra:
  source:
    instance_class: db.r6g.2xlarge    # 8 vCPU, 64 GB
    storage_gb: 400
    iops: 12000
    parameters:
      wal_level: logical
      max_wal_senders: 20
  runner:                             # the EC2 host for Connect
    instance_type: c7i.4xlarge        # 16 vCPU, 32 GB — fits a 1/2/4/8 sweep

dataset:
  initial_rows: 75_000_000
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows                    # → benchmarking/aws/seeders/cdc-rows

workload:                             # sustained writes during the bench
  write_rate_per_sec: 5000
  duration: 15m                       # framework minimum
  warmup: 2m                          # framework minimum

pipeline:                             # the Connect pipeline under test
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}            # filled from Terraform outputs
      stream_snapshot: false
      schema: public
      tables: [orders]
      slot_name: bench_slot
      batching:
        count: 5000
        period: 1s

matrix:
  cpu_points: [1, 2, 4, 8]
  overrides:
    8: { batching: { count: 10000 } }

reset:                                # between sweep points
  - sql: "SELECT pg_drop_replication_slot('bench_slot')
          FROM pg_replication_slots WHERE slot_name='bench_slot'"
```

**Framework-injected fields** (scenarios must NOT set):

```yaml
output:
  processors:
    - benchmark:
        interval: 1s
        count_bytes: true
  drop: {}

http:
  debug_endpoints: true

metrics:
  prometheus:
    add_process_metrics: true
    add_go_metrics: true

logger:
  level: INFO
```

This keeps measurement methodology uniform across all scenarios.

**Validation rules** (enforced by `runner scenario.go`):

- `workload.duration >= 15m` (rejects shorter — see "Why 15 minutes" below).
- `workload.warmup >= 2m`.
- `matrix.cpu_points` is a non-empty ascending subset of `[1, 2, 4, 8]`.
- `infra.runner.instance_type` has vCPU count `>= 2 + max(cpu_points)` (cores
  `0,1` are reserved for kernel / SSM agent / metrics scraper).
- For bounded-dataset scenarios (no `workload:` block): the scenario must set
  `dataset.expected_peak_mb_s` (an operator-supplied hint). The runner uses
  it to verify `dataset.total_bytes / expected_peak_mb_s ≥ 15 min` at the
  highest CPU point; otherwise validation fails.
- `${...}` placeholders in `pipeline` must match Terraform outputs declared by
  the named `stack`.

### Shipped scenarios

| Connector | Scenario | Customer story | Shape |
|---|---|---|---|
| postgres | `orders-cdc` | Mid-size e-commerce streams DB changes | 75M × 1.2 KB · sustained 5K writes/sec |
| postgres | `snapshot-large` | "We just turned CDC on for the first time" | 50 GB initial snapshot, then idle |
| postgres | `kafka-to-pg` | "Sink Kafka into Postgres" (RPCN vs Kafka Connect JDBC) | MSK 16 partitions · 15m sustained |
| mysql | `orders-cdc` | Same e-commerce story, MySQL binlog | 75M × 1.2 KB · 5K writes/sec |
| mysql | `snapshot-large` | Initial-snapshot ceiling | 50 GB single-table |
| sqlserver | `orders-cdc` | Single high-traffic table | 75M × 1.2 KB · 5K writes/sec |
| sqlserver | `multi-table-cdc` | "CDC 4 tables at once" (linear-scaling claim) | 4 × 20M × 1.2 KB concurrent |
| dynamodb | `orders-cdc` | Multi-shard CDC at production scale | 1 table · 10 shards · 5K writes/sec |
| dynamodb | `fanout-cdc` | "Mirror lots of small tables" | 3 tables × 5 shards |
| s3 | `archive-write` | "Stream clickstream into S3 in parquet" | Sustained 100 MB/s for 15m, ~1 MB objects |
| s3 | `batch-read` | "Reprocess a year of S3 logs" | 500 GB across 50K objects |
| iceberg | `events-append` | Continuous append to a partitioned events table | Sustained 50 MB/s · daily partitioning |
| iceberg | `large-batch-write` | One-shot historical backfill into Iceberg | 100 GB single batch |

The `cdc-rows` seeder is one binary with two code paths: a SQL path
parameterised by dialect (postgres / mysql / sqlserver) and a DynamoDB
`BatchWriteItem` path. All four CDC scenarios reuse it.

---

## Runner orchestrator

The Go binary at `benchmarking/aws/runner/` turns a scenario into a results
JSON.

### Execution flow

For `runner bench <scenario>`:

1. **Parse & validate.** Resolve `${...}` against expected Terraform outputs.
   Fail fast on the rules listed in "Validation rules" above.
2. **Build the binary.**
   `GOOS=linux GOARCH=arm64 go build -o /tmp/redpanda-connect ./cmd/redpanda-connect`.
   We benchmark the *current branch*, not a release.
3. **Provision.** `terraform apply` on `shared/` + the scenario's stack, with
   scenario fields passed as `-var`. Outputs include runner EC2 instance ID,
   source DSN/endpoint, results S3 bucket.
4. **Stage.** Upload the built binary, rendered `benchmark_config.yaml`,
   seeder binary, and `bench-step.sh` to the runner EC2 via S3 + SSM
   `RunCommand`. Stage load-generator config on a separate `c7i.large`
   load-gen EC2 so its CPU never contends with Connect.
5. **Seed.** SSM-execute the seeder on the load-gen EC2: populate
   `dataset.initial_rows` and report progress (rows/sec, bytes written) via
   stdout streaming.
6. **Sweep.** For each `n` in `matrix.cpu_points`:
   - Run the `reset` block (drop CDC slot, truncate sink, clear caches).
   - If `workload:` is set, start the sustained writer on the load-gen EC2 as
     a background process bounded to `warmup + duration` seconds.
   - Start Connect on the runner EC2, isolated:
     ```
     taskset -c 2-$((1+n)) chrt --fifo 50 \
       env GOMAXPROCS=$n GOMEMLIMIT=${n}GiB \
       /opt/bench/redpanda-connect run /opt/bench/config.yaml
     ```
     Cores `0,1` are reserved for kernel / SSM / scraper.
   - Tail Connect stdout via SSM streaming. Parse
     `rolling stats: <N> msg/sec, <M> MB/sec`. Discard `warmup` seconds;
     collect samples for `duration` seconds.
   - Stop Connect (`SIGTERM`). Compute `p5 / p50 / p95 / peak` MB/sec and
     msg/sec. Snapshot Prometheus metrics
     (`process_cpu_pct`, `go_gc_pct`, `batch_size_p50`).
   - Detect anomalies: any contiguous `>= 60s` span where MB/sec drops below
     `0.8 × p50`. Append each to a per-point `anomalies` array.
7. **Persist.** Write `results/<connector>/<scenario>/<UTC-timestamp>.json`.
   Upload the same JSON to the results S3 bucket for cross-machine sharing.
8. **Render.** Append a dated AWS section to
   `docs/benchmark-results/<connector>.md` from a Go template
   (`runner/templates/result.md.tmpl`).
9. **Refresh summary.** Recompute `docs/benchmark-results/SUMMARY.md`'s "At a
   Glance" table from the union of latest results, ranked by peak MB/sec.
10. **Tear down.** `terraform destroy` unless `keep=true` or
    (`keep-on-fail=true` and a prior step errored).

### Key choices

- **AWS Systems Manager (SSM), not SSH.** No keypair handling, no inbound port
  22, IAM-gated, audit-logged. The runner EC2 needs only the SSM agent and an
  instance profile.
- **Load generator on a separate EC2.** Sharing the Connect host would
  contaminate the CPU isolation. A `c7i.large` for the writer is enough.
- **Cores 0,1 are kernel-reserved.** Removes most OS noise while keeping the
  measured set on a single NUMA node (c7i is single-socket).
- **Default is destroy-on-failure.** AWS bills don't care that your bench
  failed. `keep-on-fail=true` is the explicit opt-out.

---

## Terraform layout

### State

S3 + DynamoDB-lock backend, one key per stack
(`shared/terraform.tfstate`, `postgres/terraform.tfstate`, …) so concurrent
benchmarks on different stacks don't lock-conflict. Backend config in
`terraform/backend.hcl`, sourced by every stack via `-backend-config`.

### Shared stack (`terraform/shared/`)

- `/16` VPC across 2 AZs, public + private subnets.
- Security groups for runner ↔ source, load-gen ↔ source, runner ↔ results
  bucket.
- The runner EC2 instance (size from scenario), the load-gen EC2
  (`c7i.large`).
- Instance profile granting SSM, S3 write to results bucket, Secrets Manager
  read.
- The results S3 bucket (versioned, lifecycle: expire raw JSON after 180d).
- An EventBridge rule + Lambda that destroys any stack whose
  `bench-session-id` tag is older than 24h — the "I closed my laptop and
  forgot" guardrail. The normal teardown path does not depend on it.

### Per-connector stacks

Each composes `shared` with the modules it needs. Variables on each stack are
the scenario's `infra.source.*` fields (instance class, storage, IOPS,
parameters, etc.). Outputs are the connection details the pipeline template
needs (`POSTGRES_DSN`, `S3_BUCKET`, `DYNAMODB_TABLES`, `KAFKA_BOOTSTRAP`,
etc.).

### Special cases

- **Iceberg** has no "instance class" knob; sizing is implicit in S3 +
  Glue. Its scenarios tune partition count and target file size instead.
- **All RDS modules** use `skip_final_snapshot = true` and
  `deletion_protection = false` since infra is ephemeral.

### Cost guardrails

- Every resource carries a `bench-session-id` tag for orphan cleanup.
- Backend `block_public_access = true` on the results bucket.
- `task aws:cost-check` prints `terraform output estimated_hourly_cost` from
  a simple per-resource lookup table so the operator knows what running
  infra is burning before invoking a sweep.

---

## Results format

### Raw JSON

`benchmarking/aws/results/<connector>/<scenario>/<UTC-timestamp>.json` (also
uploaded to the results S3 bucket):

```json
{
  "scenario": "postgres/orders-cdc",
  "scenario_hash": "sha256:…",
  "git_sha": "e491c80fc…",
  "started_at": "2026-05-19T14:02:11Z",
  "finished_at": "2026-05-19T15:33:48Z",
  "infra": {
    "runner_instance_type": "c7i.4xlarge",
    "source_instance_class": "db.r6g.2xlarge",
    "source_storage_gb": 400,
    "region": "us-east-2"
  },
  "dataset": {
    "rows": 75000000,
    "row_size_bytes": 1200,
    "total_bytes": 90000000000
  },
  "points": [
    {
      "vcpu": 1,
      "samples": [{ "t": 1, "mb_per_sec": 81, "msg_per_sec": 67000 }, "…"],
      "summary": {
        "median_mb_s": 153,
        "p5_mb_s": 144,
        "p95_mb_s": 161,
        "peak_mb_s": 167,
        "median_msg_s": 127344,
        "p5_msg_s": 119800,
        "p95_msg_s": 134000,
        "peak_msg_s": 138200
      },
      "anomalies": [
        { "start_t": 314, "duration_s": 73, "min_ratio": 0.62,
          "note": "MB/sec dropped to 0.62× median for 73s — investigate" }
      ],
      "prom_snapshot": {
        "process_cpu_pct": 96.2,
        "go_gc_pct": 4.1,
        "batch_size_p50": 4900
      }
    },
    { "vcpu": 2, "…": "…" },
    { "vcpu": 4, "…": "…" },
    { "vcpu": 8, "…": "…" }
  ]
}
```

### Rendered markdown

Appended to `docs/benchmark-results/<connector>.md`:

```md
## AWS — orders-cdc — 2026-05-19

**Scenario:** Stream changes from a 75M-row orders table at 5K writes/sec
sustained.
**Git SHA:** [`e491c80fc`](https://github.com/redpanda-data/connect/commit/e491c80fc)
**Infra:** Runner `c7i.4xlarge` (16 vCPU); Postgres `db.r6g.2xlarge`
(8 vCPU, 64 GB, 12K IOPS, 400 GB).
**Dataset:** 75M rows × 1.2 KB = ~90 GB.
**Window:** 2 min warmup + 15 min sustained 5K writes/sec.

### Throughput

| GOMAXPROCS | MB/sec (p50) | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) |
|------------|--------------|-------------|--------------|---------------|
| 1          |          153 |         144 |          161 |       127,344 |
| 2          |          257 |         241 |          268 |       214,011 |
| 4          |          358 |         339 |          372 |       298,500 |
| 8          |          386 |         362 |          401 |       321,872 |

> ⚠ At 1 vCPU: 73s dip to 0.62× median MB/sec from t=314s — possible source
> throttling. Investigate before publishing.

Raw samples + Prometheus snapshots:
[`results/postgres/orders-cdc/2026-05-19T14-02-11Z.json`](…)
```

### Rendering rules

- Append, never overwrite — history stays in git.
- Heading: `AWS — <scenario> — <YYYY-MM-DD>`. Duplicate same-day runs get
  `(run 2)`, `(run 3)` suffixes.
- Anomalies become callouts under the table; ≥ 1 anomaly per point means the
  run isn't safe to publish externally without a human note.
- Bottleneck-analysis prose is **not** auto-rendered. Authors write that in
  after the run, like the existing local results do. The template provides
  facts; humans provide interpretation.
- `docs/benchmark-results/SUMMARY.md`'s "At a Glance" table is regenerated
  programmatically — for each connector, pick the latest AWS run's `peak
  MB/sec` across scenarios.

---

## Why 15 minutes

AWS managed services routinely deliver 30–90s throughput blips:

- S3 throttle events (5xx slowdowns) during heavy PUT/LIST bursts.
- RDS storage-burst credit exhaustion on gp3 volumes under sustained writes.
- NLB target-group scaling and connection-rebalancing pauses.
- EBS gp3 burst-credit drain on the runner's root volume.
- Cross-AZ network latency variance under DRAM contention on the host
  hypervisor.

A `1m warmup + 5m duration` window lets a single 60s blip dominate the
median — the headline number becomes a measurement of the network glitch, not
of Connect.

`2m warmup + 15m duration` plus `p5 / p50 / p95` reporting:

- Lets transient throttles get absorbed without distorting the median.
- Surfaces variability (wide p5↔p95) as a first-class observation rather than
  hiding it inside an average.
- Catches sustained dips (≥ 60s) explicitly as anomalies so the operator
  knows when to dig in rather than publish.

Bounded-dataset scenarios (snapshot-large, batch-read, large-batch-write) get
sized so wall-clock completion at the *highest* CPU point is ≥ 15 min.

---

## Onboarding a new connector

The framework's value depends on this being easy. Adding a new connector is a
single PR with:

1. **Terraform module** under `terraform/modules/<source-type>/` if the source
   isn't already covered. Most new connectors reuse existing modules.
2. **Stack** under `terraform/stacks/<connector>/` composing `shared` + the
   needed module(s). Usually < 50 lines + a `variables.tf`.
3. **Seeder** (or reuse `cdc-rows`). Seeders are small Go programs under
   `benchmarking/aws/seeders/<name>/`.
4. **1–3 scenarios** under `scenarios/<connector>/<scenario>.yaml`. Minimum
   fields: `name`, `description`, `connector`, `stack`, `dataset`,
   `pipeline`.
5. **One run + committed results.** `task aws:bench -- scenario=<…>`,
   commit the rendered markdown section to
   `docs/benchmark-results/<connector>.md`, link the run JSON in the PR
   description.

No Go changes in `runner/` are required. The runner is connector-agnostic; the
YAML + Terraform stack are the per-connector contract.

`task aws:validate -- scenario=<…>` runs schema validation +
`terraform plan` + `redpanda-connect lint` on the rendered pipeline. Catches
typos before spending AWS money.

---

## Risks & open questions

- **AWS account hygiene.** This framework will create and destroy real
  resources frequently. Decision: the framework runs against a dedicated
  benchmarking AWS account (assumed pre-existing). The 24h orphan-cleanup
  Lambda is the safety net.
- **Result reproducibility.** AWS performance is not deterministic. The
  `p5/p50/p95` reporting + anomaly detection are how we surface this rather
  than pretend it doesn't exist. Re-running and seeing similar numbers within
  the p5↔p95 band is the bar for "trust this number."
- **Cost per scenario.** A 90-min single-scenario run on `c7i.4xlarge` +
  `db.r6g.2xlarge` + storage + S3 is on the order of a few dollars. A
  three-scenario session sharing infra: ~$10–15. Documented in the README;
  the cost-check task surfaces estimates pre-flight.
- **Cross-AZ vs same-AZ source/runner placement.** Default to same-AZ for
  determinism. Cross-AZ scenarios can be added later if a customer story
  needs them; today they're not in scope.
- **Migrator and Salesforce.** Explicitly out of scope this iteration. Adding
  them later means a `redpanda-cluster` module and a `salesforce-creds`
  module respectively; the framework doesn't otherwise need to change.

---

## What this is NOT

- Not a continuously-running perf-regression detector. That's a future
  iteration on top of this foundation (the JSON results are designed to feed
  such a system).
- Not a customer-facing benchmark tool. It's an internal engineering and
  marketing-number tool.
- Not a replacement for the local Docker benches. Those still serve developer
  iteration; AWS results are for the published numbers.
