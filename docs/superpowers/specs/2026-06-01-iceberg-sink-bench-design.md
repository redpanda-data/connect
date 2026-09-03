# Iceberg Sink Bench — Design

**Date:** 2026-06-01
**Status:** Approved (pending written-spec review)
**Author:** Prakhar (with Claude)

## Summary

Add the first **sink** benchmark to the Redpanda Connect AWS bench framework
(`benchmarking/aws/`): a Connect-vs-Kafka-Connect head-to-head measuring how
fast each engine sinks records from a Redpanda topic into an Apache Iceberg
table (AWS Glue catalog + S3 warehouse).

The framework today only benches CDC **sources** (`postgres_cdc`, `mysql_cdc`):
`DB → Connect → Redpanda topic`, scored on broker-side *produce* throughput. A
sink inverts the topology: `Redpanda topic → Connect → Iceberg`. That inversion
touches ~6 source-assuming spots in the runner. We refactor those behind a
`Topology` abstraction (Approach B) rather than bolting on conditionals.

## Goals

- Measure end-to-end sink throughput (rows/bytes committed to Iceberg) for
  Connect's `iceberg` output vs the Apache Iceberg Kafka Connect sink, across a
  vCPU sweep on identical hardware.
- Keep the comparison **fair** and **isolated to the sink write path** — minimize
  input-decode confounds between the two engines.
- **Do not regress** the existing CDC benches. The source path is
  refactored-not-rewritten; existing runner unit tests are the regression net.

## Non-Goals

- A generic "any sink" framework. We build the abstraction for the first sink
  (Iceberg) and let a second sink prove/refine its shape later (YAGNI).
- Avro/Schema-Registry ingestion in the primary scenario (scoped as a follow-up
  variant — see "Record format").
- Sustained-workload (live-producer) sink runs. The first sink is a bounded
  pre-seeded-topic run.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Connector category | Iceberg **sink** | First sink bench (Plan 4). |
| Data source | **Pre-seeded Redpanda topic**, bounded run | KC's Iceberg sink can only read from Kafka, so a topic is the only fair shared upstream. Bounded (no live workload) keeps the producer from becoming the bottleneck. |
| Fairness metric | **Rows/bytes committed to Iceberg** (`total-files-size` / `total-records` from snapshot summary) | True end-to-end sink completion; can't be gamed by engine self-report. Iceberg snapshot metadata tracks it natively — no S3 scan. |
| Catalog | **AWS Glue** + S3 warehouse, reached by **both engines via the Glue Iceberg REST endpoint + SigV4** | Connect's `iceberg` output only supports a REST catalog (`catalog.url` + `catalog.auth.aws_sigv4{service:"glue"}`), so KC's sink uses `iceberg.catalog.type=rest` against the *same* Glue REST URI + SigV4. Identical access path on both sides = apples-to-apples. (Corrected 2026-06-01 after reading `internal/impl/iceberg/output_iceberg.go`; earlier draft said "native glue type".) |
| Record format (primary) | **Schemaless JSON** | Isolates the measurement to the Iceberg write path: avoids the two-different-Avro-converter fairness confound and drops the Schema-Registry/shared-module dependency. |
| Record format (follow-up) | Avro + Schema Registry **variant** | Captures the "realistic decode" number; deferred. |
| Framework change | **Approach B** — `Topology` abstraction | Cleaner long-term; the latent `kcSink` direction constant was waiting for it. Accepted cost: refactors the shared render/metric core. |
| Iceberg table lifecycle | Each engine **auto-creates its own per-engine table** | Avoids pre-create schema drift between the two engines; gives trivial per-engine metric attribution. |

## Architecture: the `Topology` abstraction

A new scenario field selects a topology implementation at runner startup. The
~6 source-assuming spots are pulled behind one interface.

```go
type Direction string
const (
    DirectionSource Direction = "source" // connector reads external system → writes Redpanda (CDC)
    DirectionSink   Direction = "sink"   // connector reads Redpanda → writes external system (Iceberg)
)
// Scenario.Direction; empty string defaults to "source" → every existing CDC scenario is unchanged.

type Topology interface {
    Validate(s *Scenario) error                                                   // direction-specific field checks
    Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error) // assembles Connect input+output
    SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) // primes data
    WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) // "" for bounded sink
    ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error)    // baseline between points
    EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error)            // canonical per-engine throughput
}
```

Two implementations:

- **`sourceTopology`** — a *mechanical move* of today's `engineSpecs` +
  `renderSeedScript` / `combineReset` / `renderWorkloadScript` + the broker
  *produce* metric (`brokermetrics.go`) behind the interface. No behavior change.
- **`sinkTopology`** — new: JSON-producer seed, no workload (bounded topic),
  Iceberg-table reset, redpanda-input pipeline, Glue-snapshot metric.

### `BenchNames` — naming single-source-of-truth

Today resource names are built by scattered `fmt.Sprintf("bench_%s_%s_connect", …)`
calls across `scripts.go`, `brokermetrics.go`, and `kcconnectors.go`. Approach B
centralizes them into one `BenchNames` value computed once and passed into every
topology method: per-engine topic name, per-engine Iceberg table name, consumer
group, (future) SR subject. Single authority for naming.

### Output-shape invariant

`EngineSeries` returns `[]TopicPoint{T int /*sec*/, MBPerSec float64}` — the
**exact** type the source path already emits. Everything downstream (`stats.go`,
`render.go`, `summary.go`, `cost.go`, `PointResult`) is therefore untouched.

## Infrastructure (Terraform)

**New `terraform/stacks/iceberg/`** (mirrors `stacks/postgres/`):
- `main.tf` — composes the shared stack (`terraform_remote_state`) with a new
  `modules/glue-iceberg`.
- `variables.tf`, `outputs.tf` — exports `glue_database`, `warehouse_s3_uri`,
  `s3_bucket`, `aws_region`.

**New `terraform/modules/glue-iceberg/`** provisions:
- `aws_s3_bucket` — Iceberg data + metadata warehouse.
- `aws_glue_catalog_database` — the Iceberg namespace.
- IAM policy on the **runner** instance profile: Glue read/write + S3 read/write
  on the warehouse bucket (both engines write through the runner's role).
- **No `aws_glue_catalog_table`** — each engine creates its own per-engine table
  on first write (avoids schema drift between the two).

**Shared infra change:** `terraform/shared/runner-user-data.tftpl` — add the
download/unpack of the `iceberg-kafka-connect` plugin so KC's sink class is
available (per the kc-connector-mapping "adding a new KC plugin" step).

**Explicitly NOT changed** (because the primary scenario is schemaless JSON):
the shared `modules/redpanda/*` and `shared/outputs.tf` are left untouched — no
Schema-Registry listener. This keeps the CDC benches' shared state unperturbed.
(The Avro follow-up variant would reintroduce this additively.)

Load-gen host needs Kafka reach only (it produces JSON into the topic); the
runner host holds the Glue/S3 IAM.

## Scenario schema

New field `direction`. Sink scenarios reuse the existing **bounded-dataset**
validation path (no `workload:` block; `Validate()` already requires
`dataset.expected_peak_mb_s` and enforces a ≥15-min wall-clock floor).

```yaml
name: iceberg-orders-sink
direction: sink            # NEW — selects sinkTopology
connector: iceberg         # Connect `iceberg` output component
stack: iceberg
infra:
  runner: { instance_type: c8g.4xlarge }
  # no infra.source — the sink target (Glue+S3) is provisioned by the stack
dataset:
  initial_rows: <sized at smoke>   # records pre-produced into the topic
  row_size_bytes: 1200
  tables: [orders]                 # logical name → topic name AND iceberg table name
  seeder: json-orders              # NEW seeder
  expected_peak_mb_s: 200          # bounded-run wall-clock guard
pipeline:
  # sinkTopology.Pipeline injects the redpanda INPUT from BenchNames; the
  # scenario supplies only the connector-specific OUTPUT tuning.
  output:
    iceberg:
      # catalog/warehouse/table filled from TF outputs + BenchNames at render time
      batching: { count: 5000, period: 1s }
matrix:
  cpu_points: [1, 2, 4, 8]
reset:
  - bash: "<drop+recreate per-engine iceberg table; reset consumer-group offset to earliest>"
```

The input/output inversion (sink supplies output, source supplies input) is
owned by `Topology.Pipeline`.

### Connector registries

- **`engineSpecs`** (existing) — source connectors. Unchanged. Read by
  `sourceTopology`.
- **`sinkSpecs`** (new, parallel) — sink connectors. `sinkSpecs["iceberg"]`
  holds the Connect output component key, the per-engine Iceberg-table-name
  derivation, and the TF outputs to wire (`glue_database`, `warehouse_s3_uri`,
  `aws_region`). Read by `sinkTopology`. Preserves the "add an entry, no
  switch-statement edits" property the codebase values.

- **`kcConnectorSpecs["iceberg"]`** (the `Direction` field finally used):
  ```go
  "iceberg": {
      Class:           "org.apache.iceberg.connect.IcebergSinkConnector",
      Direction:       kcSink,
      PropsTemplate:   /* iceberg.catalog.type=glue, warehouse S3 uri,
                          value.converter=org.apache.kafka.connect.json.JsonConverter,
                          value.converter.schemas.enable=false,
                          iceberg.tables=<db>.<table> */,
      RequiredPlugins: []string{"iceberg-kafka-connect*"},
  }
  ```

## Seeder: `seeders/json-orders/`

A new Go `main` matching the existing seeder contract (staged at
`/opt/bench/<seeder>`, `seed` subcommand).

- `sinkTopology.SeedScript` injects `REDPANDA_BROKERS` (no DSN) and runs
  `json-orders seed --topic <name> --rows N --row-size B`.
- Produces N flat, fully-populated JSON records (deterministic field shape so
  type inference to Iceberg columns is consistent across both engines) using
  `franz-go` (already vendored), partitioned for parallel produce throughput so
  seeding doesn't dominate wall-clock.
- Runs **once** before the sweep — the topic is the fixed dataset; reset re-reads
  it, never re-seeds.

## Metric: Iceberg snapshot poller

The symmetric analog of the broker `/public_metrics` scrape. Added to the
per-engine bench script (runs on the runner, which holds Glue/S3 IAM):

- Every ~10 s: `aws glue get-table` → read `Table.Parameters.metadata_location`
  (Iceberg writes this) → `aws s3 cp` the `vNNN.metadata.json` →
  `jq '.snapshots[-1].summary'` for `total-files-size` and `total-records`.
- Appends `<unixtime> <total-files-size> <total-records>` to
  `iceberg-<vcpu>-<engine>.txt`, uploaded to S3 alongside other artifacts.
- Tools: `awscli` + `jq` (both already present on the runner).

`sinkTopology.EngineSeries` parses that file into `[]TopicPoint` exactly
mirroring `ParseTopicSeries`: `delta(total-files-size) / interval / MiB →
MBPerSec`, skipping counter resets. **Per-engine attribution is trivial** — each
engine writes its own table and the poller writes a per-engine file, so there's
none of the topic-prefix matching the source path's `AttributeByEngine` needs.

Engines run **sequentially** per sweep point (Connect, then KC), so the bench
script polls only the currently-running engine's table — no cross-contamination.

## Reset (between sweep points)

`sinkTopology.ResetScript` is the inverse of CDC's truncate-and-drop-slot. It
does **not** touch the topic (the fixed dataset):

1. Drop the per-engine Iceberg table — `aws glue delete-table` + delete its S3
   data/metadata prefix — so `total-files-size` restarts at 0. The connector
   recreates it on next write.
2. Reset the engine's consumer-group offset to earliest so it re-reads the whole
   topic (`kafka-consumer-groups --reset-offsets --to-earliest`).
3. For KC: idempotent connector DELETE (the source path's `combineReset`
   KC-cleanup, adapted into the sink topology).

## Sizing (one number to resolve empirically)

The bounded-run guard requires `dataset_MB / expected_peak_mb_s ≥ 15 min`. A
fixed topic is read by *every* cpu point, so a dataset sized to clear 15 min at
8 vCPU (~180 GB at 200 MB/s) takes *hours* at 1 vCPU — brutal for a smoke.

**Resolution: two scenario files.**
- `scenarios/iceberg/orders-sink.yaml` — full sweep `[1,2,4,8]`, large dataset.
- `scenarios/iceberg/orders-sink-smoke.yaml` — `cpu_points: [1]`, ~20 GB,
  `expected_peak_mb_s: 20` (so 1-vCPU still clears the 15-min floor and the seed
  is tractable).

Exact byte counts are nailed down at smoke time and recorded back into the
scenario files.

## Testing

- **Regression net:** all existing source tests (`scenario_test.go`,
  `kcconnectors_test.go`, `scripts_test.go`, `brokermetrics_test.go`) pass
  unchanged after the `sourceTopology` move.
- **New unit tests:**
  - `sinkTopology.Pipeline` renders redpanda-input + iceberg-output correctly.
  - `sinkSpecs["iceberg"]` present; `direction: sink` scenario validates.
  - `kcConnectorSpecs["iceberg"]` is `kcSink`; props template renders with
    `JsonConverter` + `schemas.enable=false`.
  - `SeedScript` / `ResetScript` render expected commands.
  - `EngineSeries` golden test from an `iceberg-<vcpu>-<engine>.txt` fixture
    (mirrors `brokermetrics_test`).
  - `BenchNames` naming.
- **Validate + 1-vCPU smoke** on the smoke scenario: two `PointResult`s, both
  `Summary.MedianMBPerSec > 0`, series populated for both engines.

## File manifest

| New | Changed |
|---|---|
| `runner/topology.go` (iface, `Direction`, `BenchNames`, selector) | `runner/scenario.go` (`Direction` field, `Validate` dispatch) |
| `runner/topology_source.go` (moved CDC logic) | `runner/scripts.go` (move fns behind `sourceTopology`) |
| `runner/topology_sink.go` | `runner/kcconnectors.go` (iceberg entry, wire `Direction`) |
| `runner/sinkspecs.go` | `runner/main.go` + `matrix.go` (call `topology.Pipeline`/`EngineSeries`; select by direction) |
| `runner/icebergmetrics.go` (+ test, fixture) | `runner/brokermetrics.go` (reachable via `sourceTopology.EngineSeries`) |
| `seeders/json-orders/` | `terraform/shared/runner-user-data.tftpl` (install `iceberg-kafka-connect` plugin) |
| `terraform/stacks/iceberg/` | |
| `terraform/modules/glue-iceberg/` | |
| `scenarios/iceberg/orders-sink.yaml` + `-smoke.yaml` | |

## Follow-ups (not in core plan)

- **Avro + Schema Registry variant** (`orders-sink-avro.yaml`): re-introduce the
  shared Redpanda SR listener (additively), an `avro-orders` seeder, and
  `AvroConverter`/`schema_registry` on both engines — for the "realistic decode"
  number.
- Update the bench-framework skill's "Sink (any) → Plan 4 TBD" row now that the
  sink path exists.
