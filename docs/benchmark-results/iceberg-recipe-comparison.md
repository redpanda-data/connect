# Iceberg Sink Throughput — Redpanda Connect vs Kafka Connect

A vCPU-scaling sweep of the `iceberg` output across two Connect tuning recipes,
head-to-head with the Kafka Connect (Tabular) Iceberg sink on identical hardware.

## Environment & methodology

- **Cloud:** AWS `us-east-2`.
- **Runner (system under test):** `c8g.4xlarge` (16 vCPU, Graviton4), pinned to
  **1 / 2 / 4 / 8 vCPU** per sweep point; memory scaled 2→4→8→16 GiB.
- **Source:** Redpanda, 3× `im4gn.2xlarge` brokers; load-gen `c8g.large`.
- **Dataset:** 160,000,000 flat-JSON "orders" rows × ~1,200 B = **~178 GB**,
  pre-seeded into a 16-partition topic, re-read from the beginning each point.
- **Sink:** Apache Iceberg via **AWS Glue REST catalog + S3**, SigV4 (`service=glue`).
- **Metric:** mean MB/s of Iceberg committed-bytes growth (`total-files-size`)
  polled from Glue — the identical measurement for both engines. 15-min window,
  0 s warmup. Mean (not median) because sink throughput is bursty.
- **Code:** redpanda-connect `62f50196b` (benthos v4.76.0); KC =
  `io.tabular.iceberg.connect` Tabular sink, Iceberg 1.5.2, 10 s coordinator commit.

## Configurations

### Recipe A — order-preserving (`scenarios/iceberg/orders-sink.yaml`)

```yaml
buffer:
  memory:
    limit: 524288000        # 500 MiB
    batch_policy:
      count: 10000
      period: 10s
output:
  iceberg:
    max_in_flight: 16
    batching:
      count: 10000
      period: 10s
    commit:
      max_snapshot_age: 24h
```

Single merged stream (no `unordered_processing`) → preserves cross-partition
order. The buffer decouples the fast input; **output-level batching** is what
feeds the commit-coalescer so `max_in_flight=16` can parallelize (without it a
single buffer stream starves the committer — 0.8 → 12.7 MB/s at 1 vCPU in smoke).

### Recipe B — max throughput, unordered (`scenarios/iceberg/orders-sink-recipe-b.yaml`)

```yaml
input:
  redpanda:
    # seed_brokers / topics / consumer_group injected by the runner
    unordered_processing:
      enabled: true
      checkpoint_limit: 1024
      batching:
        count: 10000
        period: 10s
output:
  iceberg:
    max_in_flight: 32
```

No buffer. Per-partition parallel streams feed many concurrent commits to the
coalescer, `max_in_flight=32` lets them overlap → higher ceiling, but **gives up
cross-partition ordering**.

Both target ~10k rows / ~10 s per commit (matching KC's 10 s coordinator cadence).

## Results — mean MB/s

| vCPU | Connect A | Connect B | Kafka Connect |
|------|-----------|-----------|---------------|
| 1    | 15.9      | **38.3**  | 45.2          |
| 2    | **69.1**  | 64.1      | 64.3          |
| 4    | **114.2** | 98.6      | 71.4          |
| 8    | 109.2     | **122.4** | 74.2          |

*(Bold = fastest at that vCPU. KC measured within ~2% in both sweeps → confirms
the A-vs-B comparison is apples-to-apples; shown here as the average.)*

## Analysis

- **Connect scales with cores (CPU-bound); KC plateaus.** Connect climbs
  near-linearly; KC is commit/coordination-bound and near its ceiling by 1 vCPU
  (~45 → ~74 across 8×). Crossover at 2 vCPU — above it Connect wins, up to
  **1.6× at 4 vCPU**.
- **A vs B trades the ends against the middle.** B wins at 1 vCPU (2.4×) and
  8 vCPU (steadier — A goes bursty under contention, median 49 / mean 109 at 8v);
  A edges ahead at 2–4 vCPU.
- **Why 1 vCPU is Connect's weak point:** on one core the parallel-writer design
  can't parallelize; it serializes on JSON-decode → shred → Parquet-encode,
  dominated by GC of per-record allocations. KC's coordinator-batched model
  already runs near-ceiling on one core, so it starts higher.

## Recommendation

- Ordering optional (typical for Iceberg) → **Recipe B**: big win at low core
  count, win at high core count, only a slight give-up at 2–4 vCPU.
- Cross-partition ordering required → **Recipe A** (best at 2–4 vCPU; note its
  8-vCPU burstiness).

## Caveats

- Each cell is a single 15-min steady-state run; expect ±10–20% cloud variance —
  treat sub-15% gaps as ties.
- The merged shredder allocation-caching change showed **no clean signal** vs the
  prior-code sweep (within variance; config also differed) — not claimed as a win.
- Raw per-run JSON + Prometheus snapshots are gitignored (`results/iceberg/…`);
  the auto-appended tables live in `iceberg.md`.
