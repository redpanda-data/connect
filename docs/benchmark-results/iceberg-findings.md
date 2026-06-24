# Redpanda Connect Iceberg Sink — Throughput Findings & Recommendations

## Summary
A fair, production-representative AWS benchmark of **Redpanda Connect's `iceberg`
output vs. Kafka Connect's Tabular Iceberg sink**, both writing to AWS Glue
(Iceberg REST) + S3. Headline: **Kafka Connect is more efficient at 1–2 vCPU;
Redpanda Connect scales with cores and overtakes KC at 4+ vCPU (~2× at 8 vCPU
when tuned for parallelism).** The investigation surfaced a significant
default-config trap, two effective tuning levers, and a concrete next step to
close the remaining low-core gap.

## Environment
- AWS `us-east-2`. Runner `c8g.4xlarge` (vCPU-pinned per point via `taskset`);
  3× `im4gn.2xlarge` Redpanda (RF=3); separate load generator.
- Catalog/storage: **AWS Glue Iceberg REST + SigV4** → S3. Both engines use the
  identical path (symmetric).
- Source: one 16-partition topic, **160M JSON records (~1,200 B, realistic
  high-entropy payload)**, consumed from oldest. Each engine writes its own
  pre-created Iceberg table.
- Sweep: `[1,2,4,8]` vCPU, 15-min window/point. Primary metric **records/sec**
  (committed, from Glue `total-records`, compression-independent); MB/s secondary.

## Headline results (mean records/sec)

**Connect tuned with input batching (scales for parallelism):**

| vCPU | Connect | Kafka Connect |
|------|---------|---------------|
| 1    | 15,453  | **56,525**    |
| 2    | 36,496  | **79,047**    |
| 4    | **153,405** | 85,459    |
| 8    | **178,231** | 88,527    |

**Connect tuned with a memory buffer (order-preserving):**

| vCPU | Connect | Kafka Connect |
|------|---------|---------------|
| 1    | 12,384  | **59,974**    |
| 2    | 68,329  | **84,134**    |
| 4    | **93,461**  | 89,438    |
| 8    | **100,842** | 94,084    |

- **Connect scales; KC plateaus.** KC is near-flat ~56k→94k across 8× cores (its
  single commit coordinator is the ceiling, and it is CPU-light so it does not
  need cores). Connect is CPU-bound and scales steeply.
- **Crossover at ~3–4 vCPU.** KC wins at 1–2; Connect wins at 4–8.

## The core technical discovery: default Connect is commit-latency-bound
Out of the box, Connect's iceberg output hit only **~412 rec/s at ~0.03 CPU
cores** — idle ~97% of the time. Root cause:
- Each Glue REST commit is a **serialized ~320 ms round-trip** (one committer per
  table).
- The committer **coalesces only what is already queued** (no commit-linger), and
  **backpressure** starved each commit to ~300 records.
- Result: many tiny, frequent commits → throughput pinned regardless of vCPU.

KC avoids this with a **coordinator** that commits the entire 10s interval across
all partitions in one snapshot (millions of records/commit) — maximal
amortization of the same ~320 ms cost. **The gap is records-per-commit
(~300 vs ~millions), not commit speed.**

## Two tuning levers that fix it (config only, no code)
Both make Connect CPU-bound (it then scales with cores), by feeding large batches
so each commit carries ~10 s of data (matching KC's cadence):

1. **Memory buffer** (`buffer.memory`) — decouples the fast input; **preserves
   cross-partition ordering**; plateaus ~100k rec/s (single merged stream).
2. **Input batching** (`unordered_processing.batching`) — 16 per-partition
   parallel streams → scales higher (~178k); **gives up cross-partition ordering**
   (acceptable for a sink).

**Tradeoff:** buffer = order-preserving but ~100k ceiling; input batching = ~1.8×
higher at scale but unordered. Both lift 1-vCPU from 412 → ~12–15k (≈30×) and
restore vCPU scaling.

## What we ruled out (negative results)
- **`max_in_flight` alone does nothing** without a buffer/batching (flat 416
  across 4→32) — no read-ahead means nothing accumulates to parallelize.
- **The input is not the bottleneck** — the same redpanda input into a `drop`
  output sustains **~57,000 rec/s** on one core.
- **Forcing fewer/bigger commits does NOT help low cores.** A max-coalescing
  config cut commits ~75% (32→9) but throughput stayed flat at 1 core and got
  *worse* at 2 cores (giant batches serialize the Parquet encode). **So the
  low-core deficit is not commit overhead.**

## Why KC wins at low cores (and why it is not tunable away)
At 1–2 vCPU Connect is **CPU-bound on per-record work** (JSON decode + schema
inference/shredding + Parquet encode), while KC's coordinator is CPU-light. Since
reducing commits did not help, the low-core gap is **per-record CPU efficiency,
not commits** — not fixable with batching/buffer/coalescing config. Connect's
lever is cores; KC's is its coordinator.

## Recommendations
1. **Document the tuning as the recommended Iceberg-sink config** — the default
   (small frequent commits) is a throughput trap (~412 rec/s). Ship a buffer
   (order-preserving) or input-batching (max-throughput) recipe matched to a
   sensible commit interval. *(Highest-value, zero-code.)*
2. **Consider better defaults** for the iceberg output (or a commit-linger so the
   coordinator-style coalescing happens without requiring a buffer).
3. **Investigate per-record CPU to close the low-core gap.** Profile Connect at
   1 core (`/debug/pprof`) to split JSON decode vs. type inference vs. shredding
   vs. Parquet encode vs. compression. Likely suspects: Go `encoding/json` vs
   Jackson, dynamic schemaless inference/shredding vs a declared schema, and Go
   Parquet vs the mature JVM `parquet-mr`. Test a **declared/fixed schema** path
   as a likely win.
4. **A native interval-commit coordinator** would help Connect *scale* with
   parallel writes + one commit, but — per the negative result above — likely
   will not fix the *low-core* gap (that is encoding CPU). Prioritize the profile
   first.

## Caveats / methodology
- **Delivery semantics differ and were not equalized:** KC = exactly-once (Kafka
  transactions); Connect = at-least-once. EOS carries overhead.
- **Input-batching config trades cross-partition ordering**; the buffer config
  does not.
- **n = 1** per point, single 15-min window, no warmup trimming (~1–2% dilution
  over ~90 commit cycles).
- Earlier repetitive-payload runs made MB/s meaningless (KC compressed to
  ~7 B/rec); fixed with a realistic high-entropy payload — both axes now agree
  (KC ~810 B/rec, Connect ~1,100 B/rec).
- Two mid-investigation hypotheses (`max_in_flight` helping; a coordinator closing
  the low-core gap) were **disproven by measurement** — the conclusions above
  reflect the corrected understanding.

## Raw results
- Input batching: `results/iceberg/orders-sink/2026-06-04T19-01-06Z.json`
- Buffer: `results/iceberg/orders-sink/2026-06-05T16-21-35Z.json`
