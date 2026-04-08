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
