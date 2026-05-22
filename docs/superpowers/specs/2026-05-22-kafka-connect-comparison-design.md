# AWS benchmarking: Kafka Connect head-to-head

**Date:** 2026-05-22
**Status:** Design — ready for implementation plan
**Bucket:** new (next major bench expansion; orthogonal to bucket-3 connector breadth)

## Goal

Run Kafka Connect (KC) alongside Redpanda Connect for every connector type the
bench supports, on identical hardware, against the same workload, and publish
side-by-side throughput numbers. The output is intended to be defensible enough
to put in a blog post or marketing comparison — strict apples-to-apples
fairness is the bar.

The framework covers source connectors (CDC: postgres, mysql, sqlserver, …)
and sink connectors (iceberg, s3, dynamodb, jdbc, …) under one mechanism. The
first proof-point is `postgres_cdc`; the first sink proof-point is `iceberg`.

## Non-goals

- Comparison against engines other than Kafka Connect (no Flink CDC, Airbyte,
  Striim, debezium-server-standalone, etc.).
- Multi-worker KC topologies. The bench is single-host (one runner EC2);
  multi-worker on one host would just split CPU between workers.
- KC distributed-mode HA. Irrelevant for benchmarking.
- Per-scenario pre-tuning of Debezium / Confluent connector knobs beyond
  registry defaults — the per-scenario override block exists for outliers but
  we do not pre-tune every scenario.
- Backwards compatibility with the current `output: drop` pipeline. The bench
  pivots wholesale to writing through Redpanda.

## Decisions (binding inputs)

| # | Decision | Reason |
|---|----------|--------|
| 1 | Publishable comparison | Strict fairness required; rules out any sink asymmetry |
| 2 | Sink = self-hosted Redpanda in-VPC | Cheap, deterministic, no managed-service quota cliffs; matches product story |
| 3 | KC swapped on the same runner EC2 (sequential per sweep point) | Same hardware = strongest fairness; doubles wall-clock per scenario, accepted |
| 4 | Framework for every connector on the roadmap | One registry entry per connector; mechanical to add |
| 5 | Broker-side throughput is canonical metric | Same yardstick for both engines; Connect's `benchmark_bytes_total` kept as cross-check |
| 6 | Approach C — registry-driven KC config derivation | Mirrors the existing `engineSpec` pattern; minimal scenario YAML churn |
| 7 | Sinks: continuous-rate workload by default; pre-populate as opt-in | Matches the source-side workload model; mirrors customer steady-state |

## Architecture overview

Six pieces of new or modified machinery:

| # | What | Where |
|---|------|-------|
| 1 | Redpanda cluster TF module + integration into shared stack | `benchmarking/aws/terraform/modules/redpanda/`, `terraform/shared/redpanda.tf` |
| 2 | KC install (Java, Apache Kafka tarball, Debezium + Confluent + Aiven plugins, JDBC drivers) on runner EC2 | `terraform/shared/runner.tf` user-data |
| 3 | KC connector registry (`kcConnectorSpec`) + JSON config rendering | `benchmarking/aws/runner/kcconnectors.go` (new) |
| 4 | Matrix runner gains an inner engine loop; new `--engines` flag; per-engine reset extensions | `benchmarking/aws/runner/matrix.go`, `main.go`, `scripts.go` |
| 5 | Broker-side Prometheus scraper for Redpanda topic byte-rate | `benchmarking/aws/runner/promRedpanda.go` (new) |
| 6 | Reporting: per-engine tables, head-to-head delta, SUMMARY.md cross-engine section | `templates/result.md.tmpl`, `templates/summary-section.md.tmpl`, `render.go`, `summary.go` |

Connect's pipeline output changes from `drop` to `redpanda` (the Redpanda
Connect first-party output, franz-go-backed) writing to a per-engine,
per-session topic. KC writes to its own per-engine topic. Both topics live
on the shared Redpanda cluster. Broker-side scraper attributes throughput
by topic. Cluster connection details (`seed_brokers`, TLS, SASL) live in a
top-level `redpanda:` config block, not on the output itself.

## Infra changes

### Redpanda cluster (shared)

A 3-broker Redpanda cluster on `i4i.2xlarge` (NVMe-backed, 8 vCPU / 64 GiB each).
Composed into `terraform/shared/` so it survives per-scenario teardown — tearing
down `stacks/postgres/` must not touch the broker. Provisioned with enough
headroom that it is never the bottleneck (target ~3× the highest workload rate
we currently sweep, i.e. multi-hundred-MB/s sustained on a single topic).

Listener exposed on the in-VPC private network only. Security group allows
ingress from the runner SG and the load-gen SG. Prometheus endpoint on `:9644`
accessible from the runner for scraping.

Topic naming convention (created on demand by the runner):

- Sources: `bench_<session_id>_<connector>_<engine>` — one per engine, so
  produce-side metrics attribute cleanly.
- Sinks: `bench_<session_id>_<connector>_input` — single shared input topic,
  both engines consume from it; offset reset between engines.

### Runner EC2 user-data additions

Installs to `/opt/kafka-connect/`:

- OpenJDK 21 (Amazon Corretto)
- Apache Kafka 3.8.x tarball (only for `kafka-connect.sh` + its libs)
- Debezium 2.7.x connector JARs (postgres, mysql, sqlserver — even before all
  scenarios ship, so adding a scenario only requires a YAML + registry entry)
- Confluent S3 Sink Connector 10.5.x
- Iceberg Kafka Connect sink (current ASF release coordinates pinned in user-data)
- Aiven JDBC Sink Connector (for `kafka-to-jdbc` parity scenarios)
- AWS SDK auth plugins, MariaDB JDBC driver, Postgres JDBC driver

Plugins land in `/opt/kafka-connect/plugins/`. Total install ~600 MB. The KC
worker runs in **single-worker distributed mode** as a long-lived systemd
service started by cloud-init. Connectors are submitted/deleted per sweep
point via `curl localhost:8083`, avoiding JVM startup latency inside the
warmup window. The worker process itself is what `taskset` + `chrt` pin for
each sweep point (same mechanism as today's Connect binary).

JVM heap is `-Xmx{MEM_LIMIT_GIB - 1}g` per sweep point — leaves 1 GiB for the
OS, matching how Connect's memory limit works today.

### Connect pipeline change

Today's `renderPipelineConfig` in `main.go` wires `output: drop`. New shape:

```yaml
redpanda:
  seed_brokers: ["${REDPANDA_BROKERS}"]

output:
  processors:
    - benchmark: { interval: 1s, count_bytes: true }
  redpanda:
    topic: bench_${SESSION_ID}_${CONNECTOR}_connect
```

`REDPANDA_BROKERS` is a new TF output from the shared stack. The top-level
`redpanda:` block owns connection details so both inputs and outputs can
share them — important for Plan 4's sink scenarios, where the same block
configures the input side too. The `benchmark` processor stays: its bytes/sec
rolling line is kept as a cross-check on the broker-side number. If broker-side
and `benchmark_bytes_total` diverge by > 5%, anomaly detection flags it
(probably indicates kafka client batching or a misconfigured topic).

For sink scenarios the renderer inverts: `pipeline.input` becomes a `redpanda`
input consuming from the input topic, and the connector under test sits in
`pipeline.output`. The renderer picks the shape from the registry's `Direction`.

## Registry and config rendering

New file `benchmarking/aws/runner/kcconnectors.go`:

```go
type kcDirection int
const (
    kcSource kcDirection = iota
    kcSink
)

type kcConnectorSpec struct {
    Class           string      // Java connector class
    PropsTemplate   string      // text/template, rendered to JSON for KC REST submit
    Direction       kcDirection // source = produce-side metric; sink = consume-side
    RequiredPlugins []string    // glob patterns checked at runner preflight
}

func kcConnectorSpecFor(connector string) (kcConnectorSpec, error) { … }
```

Initial registry: `postgres_cdc`, `mysql_cdc`, `iceberg`. Each template
substitutes TF outputs (DSN, broker list, session ID, destination
credentials), the scenario's table list, and the derived topic name.

**Per-scenario override** — optional top-level `kafka_connect:` block in
scenario YAML lets a scenario tune a Debezium knob without editing the
registry:

```yaml
kafka_connect:
  config:
    snapshot.mode: never
    decimal.handling.mode: string
```

Renderer is **registry template ← scenario overrides (shallow merge on the
JSON config map)**. The registry stays the source of truth for class and
required plugins; only the config dict is overrideable.

### Worker lifecycle

The KC worker process is started once (systemd `kafka-connect.service`) at
runner provisioning and stays up for the bench session. Per-sweep-point
operations are REST calls:

- `PUT /connectors/<name>/config` with the rendered JSON to create
- `DELETE /connectors/<name>` to teardown
- `GET /connectors/<name>/status` for ready-check before warmup

The worker JVM itself is what taskset pins. Submitting a new connector to an
already-running worker pinned to N cores is what we measure at each sweep
point — same as how Connect is invoked today.

## Run loop

Changes to `matrix.go`. The sweep gains an inner engine loop with `vcpu outer,
engine inner` ordering so engines run time-adjacent at each vCPU point —
producer noise affects both equivalently:

```
for vcpu n in cpu_points:
  for engine in active_engines:   # default [connect, kafka_connect]
    reset(engine):
      - SQL reset (existing engine-aware logic in scripts.go)
      - DELETE /connectors/<name> (KC only, idempotent)
      - rpk topic delete bench_<sess>_<connector>_<engine> (sources)
        OR rpk group seek-to-end <group> (sinks)
      - destination cleanup (sinks: drop iceberg table, delete S3 prefix, etc.)
    pin engine process (taskset + chrt) to n vCPUs
    start workload concurrently (load-gen, engine-appropriate mode)
    sleep(warmup); start sampling
    sample throughput, prom, redpanda metrics every 1s for `duration`
    SIGTERM engine process and workload
    upload logs to S3
    record SweepPoint{vcpu, engine, samples, anomalies, prom, redpanda}
```

A new `--engines` flag (default `connect,kafka_connect`) lets the operator
restrict the sweep to one engine. This preserves today's behavior for
regression bench runs: `--engines connect` is equivalent to the current
bench.

`SweepPoint` gains an `Engine string` field. Downstream code (`stats.go`,
`anomalies.go`, `render.go`, `summary.go`) re-indexes by `(vcpu, engine)`.

## Workload generator

For source scenarios the existing load-gen mode (writes to RDS at N rows/sec)
is unchanged.

For sink scenarios load-gen gains a `kafka-produce` mode: connect to Redpanda,
produce rows of `row_size_bytes` to the input topic at `write_rate_per_sec`.
Same rate-limiting + parallelism story as the existing RDS path.

A `pre-populate` mode (one-shot bulk produce then exit) is an opt-in for
connectors where bulk-catchup is the real workload (think Iceberg one-shot
backfills). Implemented when a scenario first needs it; **not** part of Plan 4.

## Metric collection

New `promRedpanda.go` scrapes a broker's `:9644/public_metrics` every 1s using
the existing framework from `prom.go`. Extracts
`redpanda_kafka_request_bytes_total{topic="bench_…", redpanda_request="produce"}`
for sources or
`redpanda_kafka_request_bytes_total{topic="bench_…", redpanda_request="fetch"}`
for sinks. Computes per-interval byte-rate. (Exact label names are pinned
against the Redpanda version chosen in user-data — verified during Plan 1.)

A new `RedpandaPoint` type sits next to `PromPoint` in `SweepPoint`.

The canonical published throughput number is the broker-side number.
Connect's `benchmark_bytes_total` is captured as before and reported as a
cross-check; same for KC's JMX `source-record-write-rate` (sources) or
`sink-record-read-rate` (sinks), summed across tasks. Cross-check is logged
and folded into anomaly detection (> 5% delta is suspicious).

## Reporting

### Per-scenario result markdown

Today's `templates/result.md.tmpl` becomes three tables — one per engine,
plus a head-to-head delta:

```
## Connect
| vCPU | Median MB/s | Peak | CPU% / vCPU | Anomalies |

## Kafka Connect
| vCPU | Median MB/s | Peak | CPU% / vCPU | Anomalies |

## Head-to-head
| vCPU | Connect MB/s | KC MB/s | Ratio (Connect / KC) | Winner |
```

The existing render tests cover the Connect-only shape and assert column
widths byte-exact. To avoid mutating that fixture (and losing the regression),
new tests cover the dual-engine shape with their own golden fixture.

### SUMMARY.md auto-refresh

A new section ("Connect vs Kafka Connect") sits underneath the existing
throughput table:

```
| Connector | Scenario | Connect best (MB/s @ vCPU) | KC best (MB/s @ vCPU) | Connect/KC |
| postgres_cdc | orders-cdc | 99 @ 4 | 38 @ 2 | 2.6× |
```

`walkResults` already discovers latest JSON per `(connector, scenario)`;
`derivedRow` learns about engine and emits two numbers per row.

### JSON result format

`PointResult` gains an `Engine string` field ("connect" | "kafka_connect").
`WriteResultJSON` already serialises arbitrary `PointResult`s; no schema
change beyond the new field. Existing test fixtures update to include
`engine: connect`.

### Anomaly detection

`DetectAnomaliesWithProm` runs per-engine. One new cross-engine check
compares the two engines' broker-side throughput at the same vCPU point and
flags points where the gap is far from the run-wide mean (could indicate one
engine hit a producer-bound moment the other didn't).

## Scope and plan decomposition

This design is too large for a single implementation plan. Decompose into
four sequential plans, each leaving the bench in a working state:

### Plan 1 — Redpanda + KC infra; no behavior change

- Redpanda module + integration into shared stack
- KC install on runner EC2 (Java, AK tarball, all plugins, systemd unit)
- Broker-side scraper (`promRedpanda.go`) wired into matrix runner but its
  output ignored by reporting
- Connect's pipeline stays writing to `drop` — bench results identical to
  pre-plan numbers

Acceptance: `task aws:bench` produces the same row as before; manual
`curl :8083/connector-plugins` on the runner shows Debezium classes loaded;
`curl :9644/public_metrics` on a broker returns the byte-rate metric.

### Plan 2 — Engine swap for source connectors

- `kcconnectors.go` + registry (`postgres_cdc`, `mysql_cdc` initially)
- `renderKCConfig` function + per-scenario override merge
- Engine-inner sweep loop + `--engines` flag
- Connect's pipeline output changes from `drop` to `redpanda` (with the top-level `redpanda:` block for connection details)
- Per-engine topic naming + reset extensions (rpk topic delete, KC connector DELETE)

Acceptance: postgres and mysql scenarios produce side-by-side numbers for
both engines on the same sweep; published markdown shows both columns.

### Plan 3 — Reporting and anomalies

- New `result.md.tmpl` shape (per-engine tables + head-to-head)
- New SUMMARY.md cross-engine section
- `PointResult.Engine` plumbed through `render.go` + `summary.go`
- Cross-engine anomaly check + broker-vs-native cross-check

Acceptance: published `docs/benchmark-results/postgres.md` shows the
three-table format with real numbers from a Plan 2 run; SUMMARY.md auto-refreshes
the cross-engine section after a bench.

### Plan 4 — Sink support, Iceberg pilot

- Pipeline-shape flip in renderer (sink scenarios use `input: redpanda`,
  output is the connector under test)
- Load-gen `kafka-produce` mode
- Broker-side consume-direction metric
- New TF stack `terraform/stacks/iceberg/` (Glue catalog + S3 bucket)
- Iceberg registry entry (both engines)
- Scenario YAML `scenarios/iceberg/orders-sink.yaml`

Acceptance: iceberg scenario produces side-by-side numbers; consume-side
broker metric is the published number; reset between engines correctly
drops the destination Iceberg table.

### Beyond Plan 4

Per-connector follow-ups (each = registry entry + scenario YAML + TF stack
if not already present):

- sqlserver_cdc (source)
- dynamodb sink
- s3 sink
- kafka-to-pg (JDBC sink)

These are each smaller than Plan 1–4 and ship independently.

## Open decisions to pin during Plan 1

| Decision | Resolution path |
|----------|-----------------|
| Redpanda version | Pin current stable (v25.x as of 2026-05) in the TF module AMI / install step |
| KC / Apache Kafka version | Pin 3.8.x |
| Debezium version | Pin 2.7.x (compatible with AK 3.8) |
| Confluent S3 Sink version | Pin 10.5.x |
| Iceberg KC sink coordinates | Verify current ASF release + pin in user-data |
| JVM heap formula | `-Xmx{MEM_LIMIT_GIB - 1}g`, same as Connect's mem limit |
| KC distributed-mode config | Single worker; `tasks.max=1` for Debezium sources; JSON converters |
| Redpanda metric label names | Verify exact label key/value (`redpanda_request` vs alternate naming) in Plan 1 |

## Risks and mitigations

| Risk | Mitigation |
|------|-----------|
| Doubling wall-clock per scenario makes runs expensive | `--engines connect` preserves single-engine runs for regression bench; cost-check Taskfile entry already exists |
| KC connector plugin versions drift / break independently of Connect | Pin all plugin versions in runner user-data; bump deliberately, never `:latest` |
| Debezium single-task model caps KC throughput at low vCPU | This is a real product comparison point. Document it in the scenario; do not "fix" it by adding tasks (would not match how customers run KC) |
| Redpanda broker becomes the bottleneck for high-throughput sinks | Pre-flight check: scraper publishes broker CPU% per sweep point; if > 60% sustained, anomaly fires and run is invalidated |
| Per-engine topic naming collisions across concurrent runs | Topic name includes session ID; existing orphan-cleanup Lambda extends to delete bench topics older than TTL |
| Cross-check divergence (broker vs native metric) signals a real bug | Anomaly check at 5% delta surfaces it; do not silently paper over |

## What this design deliberately does not cover

- Pre-populate workload mode for sinks (deferred; opt-in once a scenario needs it).
- Multi-broker scaling on the Redpanda cluster (one cluster sized for headroom; not a tunable per scenario).
- Cross-region benchmarking (everything stays in one region, like today).
- KC schema registry integration (we use the embedded JSON converter; no Avro / Protobuf).
- Connect license differences (community vs cloud vs full distribution). All bench runs use the full `redpanda-connect` binary, same as today.

## Naming gotchas

- `kcConnectorSpec` (this design) is distinct from `engineSpec` (existing,
  in `scripts.go`, keyed by source DB engine). Both exist concurrently:
  `engineSpec` is keyed by connector name and tells the runner how to talk to
  the source DB (psql vs mysql CLI, DSN env var, etc.); `kcConnectorSpec` is
  keyed by connector name and tells the runner the KC counterpart's class
  and config template. Do not collapse them.
- Per-engine topic naming uses underscores not dashes (Kafka topic name rules
  permit both, but underscores match the existing convention in scenario
  table names).
