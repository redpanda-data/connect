# Iceberg Benchmark Results

Benchmarks for the `iceberg` output using a local REST catalog backed by MinIO (S3-compatible).

See [`internal/impl/iceberg/bench/`](../../internal/impl/iceberg/bench/) for the benchmark configs and run instructions.

## Write Throughput — CPU & Batch Size Scaling

Synthetic events generated at maximum speed (`generate` input with `count: 0, interval: ""`), written to a single Iceberg table. Varying `GOMAXPROCS` and `batching.count`.

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.87.2), x86_64, MinIO + REST catalog running in Docker (localhost)

**Dataset:** Synthetic events, ~142 B per message (id, user_id, event_type, value, info, ts) — measured at the pipeline processor. Actual bytes written to MinIO will differ due to Parquet columnar compression.

**Count:** 1,000,000 messages per run

### msg/sec

| GOMAXPROCS  | batch=1000 | batch=5000 | batch=10000 |
|-------------|------------|------------|-------------|
| 1           |        757 |      3,105 |       5,442 |
| 2           |      1,186 |      4,408 |       6,763 |
| 4           |      1,147 |      4,758 |       8,483 |
| 8           |      1,056 |      4,107 |       8,231 |
| (unbounded) |            |            |             |

### kB/sec (batch=1000, batch=5000) / MB/sec (batch=10000)

| GOMAXPROCS  | batch=1000 | batch=5000 | batch=10000 |
|-------------|------------|------------|-------------|
| 1           |        106 |        435 |         774 |
| 2           |        166 |        618 |         961 |
| 4           |        161 |        667 |        1206 |
| 8           |        148 |        576 |        1170 |
| (unbounded) |            |            |             |

**Observations:**

- **Batch size is the dominant factor:** throughput at 1 core scales from 757 (batch=1000) → 3,105 (batch=5000) → 5,442 (batch=10000) msg/sec. Each batch = one catalog commit round-trip, so fewer commits = dramatically higher throughput.
- **batch=5000 and batch=10000 benefit from more cores up to 4**, then regress at 8 — the catalog commit overhead is reduced enough that CPU parallelism helps, but 8 cores reintroduces contention.

---

## Write Throughput — Batch Size & max_in_flight Scaling

Fixed at `GOMAXPROCS=4`, varying `batching.count` and `max_in_flight` to measure the impact of concurrent catalog commits.

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.87.2), x86_64, MinIO + REST catalog running in Docker (localhost)

**Dataset:** Synthetic events, ~142 B per message

**Count:** 1,000,000 messages per run

### msg/sec

| max_in_flight | batch=5000 | batch=10000 |
|---------------|------------|-------------|
| 4             |      4,758 |       8,483 |
| 8             |      7,105 |      13,839 |
| 16            |     12,973 |      23,316 |
| 32            |     20,462 |      34,835 |
| 64            |     34,993 |      33,703 |
| 128           |     33,911 |      33,742 |

### MB/sec

| max_in_flight | batch=5000 | batch=10000 |
|---------------|------------|-------------|
| 4             |       0.67 |        1.21 |
| 8             |       1.00 |        2.00 |
| 16            |       1.80 |        3.30 |
| 32            |       2.90 |        5.00 |
| 64            |       5.00 |        4.80 |
| 128           |       4.80 |        4.80 |

**Observations:**

- **`max_in_flight` is the most impactful knob:** at batch=10000, throughput scales from 8,483 (MIF=4) → 13,839 (MIF=8) → 23,316 (MIF=16) → 34,835 (MIF=32) msg/sec — a 4x gain by increasing concurrent commits.
- **The ceiling is ~34K msg/sec / 5 MB/sec**, hit at MIF=32 for batch=10000 and MIF=64 for batch=5000. This is the MinIO throughput limit, not the connector.
- **batch=5000 and batch=10000 converge at high MIF values** — both plateau at ~34K msg/sec when given enough concurrent commits. batch=10000 reaches the ceiling with fewer in-flight requests (MIF=32 vs MIF=64).
- **Sweet spot: batch=10000, MIF=32** — reaches maximum throughput with the least concurrency overhead.
- The fundamental insight from both sections: the Iceberg write bottleneck is catalog commit latency. The connector itself is not the bottleneck — throw more concurrent commits at it (`max_in_flight`) and it scales linearly until MinIO saturates.

---

## Comparison: Kafka Connect vs Redpanda Connect Iceberg

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.87.2), x86_64
**Dataset:** 10,000,000 synthetic events, MinIO + Iceberg REST catalog in Docker

Both connectors use a 10s commit window and 16 Kafka partitions. The transformation computes 5 derived fields per message (`event_id`, `value_usd`, `value_tier`, `ts_ms`, `is_high_value`).

### Results

#### Sink only

| Connector                | Throughput    |
|--------------------------|---------------|
| Kafka Connect (Tabular)  | 84,745 msg/s  |
| Redpanda Connect         | 61,349 msg/s  |

#### Transform + sink

| Connector                | Kafka CPUs | Throughput    |
|--------------------------|------------|---------------|
| Kafka Connect (Tabular)  | unbounded  | 37,037 msg/s  |
| Redpanda Connect         | unbounded  | 47,272 msg/s  |
| Redpanda Connect         | 1          | 45,248 msg/s  |
| Redpanda Connect         | 2          | 48,829 msg/s  |

### Notes

- **Kafka Connect sink-only** is fastest in isolation — 16 tasks consuming pre-transformed data directly into Iceberg.
- **Kafka Connect with transformation** requires a separate RPCN pre-processing step that writes to an intermediate Kafka topic (`bench-events-transformed`), then Kafka Connect reads from that topic and sinks to Iceberg. The two-stage I/O cuts throughput by more than half.
- **Redpanda Connect** handles transformation and Iceberg writes in a single pipeline — no intermediate topic, no extra Kafka round-trip.
- **End-to-end (the realistic scenario):** Redpanda Connect is ~1.3x faster than Kafka Connect (47k vs 37k msg/s).


## AWS — orders-sink-smoke — 2026-06-02

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`66ae12d30`](https://github.com/redpanda-data/connect/commit/66ae12d306c8a1c0f20857da1c7eb3b45d6ea349)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |            0 |            0 |           0 |            1 |             0 |                    |
| 1          | kafka_connect |            0 |            0 |           0 |            0 |             0 | -0 MB/s (-100%)    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-02T21-39-10Z.json`](results/iceberg/orders-sink-smoke/2026-06-02T21-39-10Z.json)


## AWS — orders-sink-smoke — 2026-06-03

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`66ae12d30`](https://github.com/redpanda-data/connect/commit/66ae12d306c8a1c0f20857da1c7eb3b45d6ea349)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | kafka_connect |            0 |            0 |           0 |            0 |             0 |                    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-03T01-39-56Z.json`](results/iceberg/orders-sink-smoke/2026-06-03T01-39-56Z.json)


## AWS — orders-sink-smoke — 2026-06-03

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`c95688732`](https://github.com/redpanda-data/connect/commit/c9568873253d6a655c28aa0f6e314f20f6cfe57a)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | kafka_connect |            0 |            0 |           0 |            1 |             0 |                    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-03T03-50-16Z.json`](results/iceberg/orders-sink-smoke/2026-06-03T03-50-16Z.json)


## AWS — orders-sink-smoke — 2026-06-03

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`43fba5b8c`](https://github.com/redpanda-data/connect/commit/43fba5b8cd438e473e06cc4f36fb630385727b14)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |            0 |        0.460 |            0 |           0 |            1 |             0 |                    |
| 1          | kafka_connect |            0 |        0.096 |            0 |           0 |            1 |             0 | -0 MB/s (-100%)    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-03T04-41-41Z.json`](results/iceberg/orders-sink-smoke/2026-06-03T04-41-41Z.json)


## AWS — orders-sink — 2026-06-03

**Scenario:** Drain a pre-seeded Redpanda topic of flat JSON records into an Apache Iceberg
table (AWS Glue REST catalog + S3) and compare Connect's iceberg output against
the Kafka Connect Iceberg sink, head-to-head across a vCPU sweep. Throughput is
the Iceberg table's committed-bytes growth (total-files-size), polled from Glue.
Both engines reach Glue via the same REST endpoint + SigV4 (service=glue), so
the comparison is apples-to-apples. Bounded dataset (no sustained workload):
the topic is the fixed input; each sweep point re-reads it from the beginning.

**Git SHA:** [`d42480d5b`](https://github.com/redpanda-data/connect/commit/d42480d5bf7b0b85443722cb3079e8033862168f)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 160,000,000 rows × 1200 B = ~178 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |            0 |        0.460 |           412 |            0 |           0 |            0 |           416 |                    |
| 1          | kafka_connect |            1 |        0.770 |       104,828 |            1 |           0 |            1 |       103,181 | +0 MB/s (+64%)     |
| 2          | connect       |            0 |        0.462 |           414 |            0 |           0 |            0 |           416 |                    |
| 2          | kafka_connect |            1 |        0.961 |       130,845 |            1 |           1 |            2 |       125,045 | +0 MB/s (+98%)     |
| 4          | connect       |            0 |        0.462 |           414 |            0 |           0 |            0 |           416 |                    |
| 4          | kafka_connect |            1 |        0.959 |       130,534 |            1 |           1 |            2 |       124,727 | +0 MB/s (+98%)     |
| 8          | connect       |            0 |        0.461 |           413 |            0 |           0 |            0 |           416 |                    |
| 8          | kafka_connect |            1 |        0.994 |       135,103 |            1 |           1 |            2 |       128,772 | +0 MB/s (+104%)    |


### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
| 8    | kafka_connect | connect       | 2.04x |           1 |           0 |

Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink/2026-06-03T17-35-05Z.json`](results/iceberg/orders-sink/2026-06-03T17-35-05Z.json)


## AWS — orders-sink-smoke — 2026-06-03

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`d42480d5b`](https://github.com/redpanda-data/connect/commit/d42480d5bf7b0b85443722cb3079e8033862168f)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |            0 |        0.046 |            41 |            0 |           0 |            0 |            37 |                    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-03T20-11-41Z.json`](results/iceberg/orders-sink-smoke/2026-06-03T20-11-41Z.json)


## AWS — orders-sink-smoke — 2026-06-03

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`d42480d5b`](https://github.com/redpanda-data/connect/commit/d42480d5bf7b0b85443722cb3079e8033862168f)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |            0 |        0.462 |           414 |            0 |           0 |            0 |           416 |                    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-03T20-33-55Z.json`](results/iceberg/orders-sink-smoke/2026-06-03T20-33-55Z.json)


## AWS — orders-sink-smoke — 2026-06-04

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`d42480d5b`](https://github.com/redpanda-data/connect/commit/d42480d5bf7b0b85443722cb3079e8033862168f)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           10 |       12.131 |        10,984 |           10 |           0 |           33 |         9,094 |                    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-04T04-00-16Z.json`](results/iceberg/orders-sink-smoke/2026-06-04T04-00-16Z.json)


## AWS — orders-sink-smoke — 2026-06-04

**Scenario:** 1-vCPU smoke for the iceberg sink bench (Connect + Kafka Connect). Small
pre-seeded dataset sized so a single vCPU still clears the 15-minute floor at
a conservative ~15 MB/s estimate. Use this to validate the Glue REST + SigV4
path on both engines before the full sweep.

**Git SHA:** [`d19c10b7d`](https://github.com/redpanda-data/connect/commit/d19c10b7d36c0ee92ea45c03594325938043a2f5)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           11 |       12.303 |        11,139 |           11 |           0 |           32 |        10,003 |                    |


Raw samples + Prometheus snapshots: [`results/iceberg/orders-sink-smoke/2026-06-04T14-53-30Z.json`](results/iceberg/orders-sink-smoke/2026-06-04T14-53-30Z.json)
