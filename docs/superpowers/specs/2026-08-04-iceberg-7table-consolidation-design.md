# Iceberg 7-table consolidation: can 7 one-core pipelines become one process?

**Date:** 2026-08-04
**Status:** approved, implementing
**Scope:** `benchmarking/aws/` — multi-topic support for the sink bench, a fan-in
arm mode, and one new scenario.

## Customer question

A customer runs **7 topics → 7 Iceberg tables** as **7 separate pipelines, 1 core
each = 7 cores**. They want fewer total cores. Can they consolidate onto one
Connect process, and at what core count does throughput hold?

Two candidate topologies:

- **Streams mode** — one process, 7 streams, stream *i* reads topic *i* and writes
  table *i*. Independent commit paths; costs 7 consumer clients and 7 buffers.
- **Fan-in** — one process, one pipeline subscribed to all 7 topics, one `iceberg`
  output routing by interpolated table name. One client, one buffer.

## What is already known, and why it frames the test

From `docs/benchmark-results/iceberg-recipe-comparison.md` and the 2026-08-04
arms run:

| cores | MB/s | per core |
|---|---|---|
| 1 | 15.9 (Recipe A) / 38.3 (Recipe B) | 16-38 |
| 2 | 69.1 (68.3 reproduced in-session) | ~34 |
| 4 | 114.2 | ~29 |
| 8 | 109-122 | ~14 |

- **A single process plateaus at ~114-122 MB/s.** If the customer's aggregate
  exceeds that, no consolidation onto one process is possible at any core count —
  the answer becomes "shard across 2+ processes", and this test tells them the
  per-process budget.
- **1-core pipelines are this sink's worst operating point**, and the Recipe A vs B
  spread there is 2.4×. If their 7 pipelines are ordered/Recipe-A-shaped they are
  likely near 16 MB/s each (~112 MB/s total), in which case **one 4-core process
  matches their entire current throughput at 43% of the cores.**
- Per-core efficiency peaks around 2 cores, so 2 and 4 are the core points worth
  measuring. 1 core is not a credible consolidation target for 7 tables and is
  excluded.

`router.go:141-166` groups each batch by table key and issues one write per table,
so fan-in does not degenerate to per-row writes. But those writes **loop
sequentially** within a batch, so fan-in must use a proportionally larger batch
(×7) and depends on `max_in_flight` for pipelining. That asymmetry is the thing
the test actually probes.

## Design decision that keeps the comparison honest

Both topologies must write the **same 7 tables**, so the metric sidecar sums an
identical table set for every arm and the reset creates one set.

Table naming is therefore derived from the **topic index**, not the stream index:

```
topic  bench_<sess>_iceberg_src_t0   ->  table  bench_<sess>_iceberg_connect_t0
topic  bench_<sess>_iceberg_src_t6   ->  table  bench_<sess>_iceberg_connect_t6
```

- **Streams mode:** stream *i* is handed a `BenchNames` scoped to topic *i*.
- **Fan-in:** one output, `table` interpolated from the topic metadata:
  `${! @kafka_topic.replace_all("-","_").replace_all("_src_t","_connect_t") }`

Both produce the identical 7 table names. Verified inputs: `kafka_topic` metadata
is set by `franz_reader.go:405`; Glue identifiers cannot contain `-`, hence the
dash replacement.

## Arms

`cpu_points: [2, 4]`, arms `streams7` and `fanin` → **4 measured points**.
Connect-only. All arms hold total resources constant so this is a topology
comparison, not a resource one:

| arm | streams | per-stream buffer | per-stream `max_in_flight` | batch |
|---|---|---|---|---|
| `streams7` | 7 | 71 MiB (500/7) | 2 | 10000 / 10s |
| `fanin` | 1 | 500 MiB | 16 | 70000 / 10s (×7, per the sequential-write finding) |

The customer's current baseline is **not** re-measured — it is 7 independent
single-core processes, which this framework runs one process at a time. The
published 1-core numbers (15.9 / 38.3 MB/s) bracket it, and the calibration
question is better answered from the customer's own metrics than by a $6 run.
Getting their actual per-pipeline throughput remains the single most decisive
input, and this test is worth interpreting only alongside it.

## Implementation

### 1. `dataset.topics` (default 1)

New `DatasetSpec.Topics int`. `1` (or absent) preserves every existing scenario
byte-for-byte. When `> 1`:

- `initial_rows` remains the **total**, split evenly across topics. Validation's
  bounded-dataset math is unchanged because it works off the total.
- Validate: `Topics >= 1`, `Topics <= 16`, and `initial_rows % Topics == 0` so the
  split is exact and the arithmetic is auditable.

### 2. `BenchNames` topic-indexed names

- `Topics int`, `TopicIndex int`, `WithTopics(n)`, `WithTopic(i)`.
- `SourceTopic()` returns `..._src` when `Topics <= 1` (unchanged) else
  `..._src_t<TopicIndex>`.
- `IcebergTable(engine)` gains the same `_t<i>` suffix rule. **The existing
  `_s<i>` stream suffix stays** for the single-topic arms scenario; the two are
  mutually exclusive and validation enforces that.
- `IcebergTablesForTopics(engine)` → all N topic-derived tables, used by the
  sidecar and the reset.
- `ConsumerGroup(engine)` gains `_t<i>` when `Topics > 1` **for streams mode**, so
  each stream has its own group. Fan-in uses the unsuffixed group with 7
  subscriptions.

### 3. Seed script loops

`sinkTopology.SeedScript` emits one seeder invocation per topic when `Topics > 1`,
each with `--rows=<total/N>` and `--partitions=<dataset.partitions_per_topic>`
(new field, default 4 — 7×4 = 28 partitions is ample for ≤4 cores and seeds far
faster than 7×16). No seeder code change: `json-orders` already accepts `--topic`,
`--rows`, `--partitions` and pre-creates with RF 3.

### 4. Fan-in arm mode

New `Arm.FanIn bool`. When true the arm renders a single config whose redpanda
input lists **all** N topics under one consumer group, and whose `iceberg` output
sets the interpolated `table` above. Mutually exclusive with `Streams > 1`;
validation rejects the combination.

### 5. Reset and sidecar cover the topic-derived table set

`ResetScript` pre-creates the union of the N topic tables (per engine), reusing the
existing bounded-retry tablegen. `MetricSidecar` sums `IcebergTablesForTopics`, and
keeps emitting per-table `table_files_size_bytes` lines — which here are far more
valuable than in the 2-stream run, because with 7 tables a single starved topic is
otherwise invisible in the sum.

### 6. Lift the single-`cpu_points` restriction for arms

`Validate` currently requires exactly one `cpu_points` entry when arms are present.
That was right for a fixed-pin A/B; it is wrong here, where topology × core count
is the whole question. Allow multiple entries and let the plan expand
`cpu_points × arms`, which `buildSweepPlan` already does. Keep a guard on the
product (`<= 8` points) so a careless scenario cannot silently commit to a
day-long run.

## Sizing

**119,000,000 rows total ÷ 7 = 17,000,000 per topic, 1200 B → ~143 GB.**

`Validate` computes `total_MiB / expected_peak_mb_s >= 900 s`: 136,185 MiB / 145 =
939 s. So `expected_peak_mb_s: 145`, which also sits above the ~122 MB/s observed
process ceiling, so the topic cannot drain mid-window even if fan-in surprises us.

## Verification

- Unit tests for: `Topics` default/validation, topic-indexed naming, the 7-topic
  seed script, fan-in config rendering (all 7 topics subscribed, interpolated table
  correct), reset covering 7 tables, sidecar summing 7 tables.
- **Parity: every existing scenario must be unaffected.** `Topics` defaults to 1;
  the six source scenarios and the two single-topic iceberg scenarios must render
  byte-identically. This is the primary regression guard.
- `task aws:validate` on the new scenario.
- A local streams-mode config-shape check as in the previous plan.

## Acceptance for the run

- All 4 points produce non-zero throughput.
- **Every one of the 7 tables grows in every arm.** With 7 tables the summed metric
  can hide a starved topic; the per-table lines are the only way to see it.
- Fan-in's per-table byte spread should be roughly even. A skew means the
  interpolated routing or the batch grouping is favouring some tables.
- Report per-arm mean, p50, p5, p95 — p5 matters as much as the mean, per the
  2026-08-04 finding that topology changed stability more than ceiling.

## Out of scope

- Re-measuring the customer's 7×1-core baseline (needs 7 concurrent processes).
- Asymmetric tenants (different row sizes / rates per topic) — the obvious next
  test, and the one that probes whether a hot topic starves a cold one.
- Kafka Connect comparison.
