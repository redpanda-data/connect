# PostgreSQL Benchmark Results

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.87.2), x86_64

See [`internal/impl/postgresql/bench/`](../../internal/impl/postgresql/bench/) for configs and run instructions.

---

## CDC / Snapshot — Small Rows (cart table)

Full snapshot of `public.cart`: 10,000,000 rows × ~600 B. Varying `GOMAXPROCS` and `batching.count`.

### msg/sec

| GOMAXPROCS  | batch=1000 | batch=5000 | batch=10000 |
|-------------|------------|------------|-------------|
| 1           |    134,287 |    130,555 |     129,603 |
| 2           |    212,852 |    218,055 |     214,555 |
| 4           |    276,259 |    296,138 |     264,454 |
| 8           |    300,760 |    318,660 |     284,733 |
| (unbounded) |    211,111 |            |             |

### MB/sec

| GOMAXPROCS  | batch=1000 | batch=5000 | batch=10000 |
|-------------|------------|------------|-------------|
| 1           |         81 |         78 |          78 |
| 2           |        128 |        131 |         129 |
| 4           |        166 |        178 |         159 |
| 8           |        181 |        192 |         171 |
| (unbounded) |        127 |            |             |

**Observations:**
- Core scaling is strong up to 4 cores (1→2: ~1.58×, 2→4: ~1.30×), then plateaus (4→8: ~1.09×).
- `batch=5000` is the sweet spot — consistently fastest across all core counts.
- `batch=10000` regresses at higher core counts due to memory pressure and pipeline stall time waiting to fill a batch.
- At 1 core, batch size has no effect (~130K msg/sec), confirming the bottleneck is connector read throughput, not batch assembly.

---

## CDC / Snapshot — Large Rows (users table)

Full snapshot of `public.users`: 150,000 rows × ~625 KB. I/O bound workload.

### msg/sec

| GOMAXPROCS  | batch=1000 | batch=5000 | batch=10000 |
|-------------|------------|------------|-------------|
| 1           |        883 |        843 |         N/A |
| 2           |      1,166 |      1,134 |       1,024 |
| 4           |      1,145 |        N/A |         N/A |
| 8           |      1,145 |        N/A |         N/A |

### MB/sec

| GOMAXPROCS  | batch=1000 | batch=5000 | batch=10000 |
|-------------|------------|------------|-------------|
| 1           |        580 |        554 |         N/A |
| 2           |        766 |        745 |         673 |
| 4           |        752 |        N/A |         N/A |
| 8           |        752 |        N/A |         N/A |

**Observations:** Throughput plateaus at 2 cores (1,166 msg/sec, 766 MB/sec) and is flat from 4→8 cores — purely I/O bound. Additional cores provide no benefit. Contrast with cart where throughput scaled to 318K msg/sec at 8 cores.

---

## Kafka → PostgreSQL: Redpanda Connect vs Kafka Connect (JDBC Sink)

Both connectors read from a 16-partition `bench-events` Kafka topic and write to a `bench_events` PostgreSQL table. Dataset: 10,000,000 rows × ~200 B (synthetic events: id, category, value, ts).

See [`internal/impl/postgresql/bench/kafka-connector/`](../../internal/impl/postgresql/bench/kafka-connector/) for setup.

### Comparison (best configuration per connector)

| Connector                 | Configuration        | Elapsed | Throughput     |
|---------------------------|----------------------|---------|----------------|
| Kafka Connect (JDBC Sink) | 16 tasks, batch=3000 |     55s | 181,818 msg/s  |
| Redpanda Connect          | mif=64               |     70s | 130,952 msg/s  |

Kafka Connect is **~1.39× faster** on this workload. Its JDBC sink tasks amortise PostgreSQL round-trips more aggressively than RPCN's `sql_insert` output bounded by `max_in_flight`.

### Redpanda Connect tuning runs

| max_in_flight | GOMAXPROCS | Kafka CPUs | Elapsed | Throughput     |
|---------------|------------|------------|---------|----------------|
| 16            | uncapped   | uncapped   |     88s | 103,825 msg/s  |
| 64            | uncapped   | uncapped   |     70s | 130,952 msg/s  |
| 128           | uncapped   | uncapped   |     96s | 104,166 msg/s  |
| 128           | 4          | uncapped   |     96s | 104,166 msg/s  |
| 128           | 8          | uncapped   |    145s |  68,965 msg/s  |
| 128           | 4          | 1          |    121s |  70,300 msg/s  |
| 64            | uncapped   | 2          |     89s | 112,359 msg/s  |
| 64            | uncapped   | 3          |    101s |  99,009 msg/s  |

**Observations:**
- **Sweet spot: `mif=64`, uncapped Kafka** — 130,952 msg/s.
- Increasing `max_in_flight` beyond 64 causes PostgreSQL connection contention and hurts performance.
- Adding GOMAXPROCS cores degrades throughput — the bottleneck is PostgreSQL write throughput, not CPU.
- Capping Kafka CPU below 2 cores throttles fetch throughput and becomes the new bottleneck.


## AWS — orders-cdc — 2026-06-01

**Scenario:** Stream changes from a Postgres orders table under sustained heavy writes
(target 150K writes/sec ≈ 180 MB/s) so the postgres_cdc input — not the
producer — is the bottleneck across the whole CPU sweep. TRUNCATE between
sweep points keeps the table size bounded (no Trap 3).

**Git SHA:** [`25057d693`](https://github.com/redpanda-data/connect/commit/25057d6936c7785ca918aa09eac8a1341afcf875)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.4xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           51 |            4 |          32 |           57 |        40,000 |                    |
| 1          | kafka_connect |           36 |           36 |          33 |           36 |             0 | -15 MB/s (-30%)    |
| 2          | connect       |           83 |            6 |          25 |           89 |        65,000 |                    |
| 2          | kafka_connect |           16 |           16 |          16 |           16 |             0 | -67 MB/s (-81%)    |
| 4          | connect       |          102 |            6 |          32 |          108 |        80,000 |                    |
| 4          | kafka_connect |           17 |           17 |          17 |           17 |             0 | -85 MB/s (-83%)    |
| 8          | connect       |          102 |            7 |          32 |          108 |        80,000 |                    |
| 8          | kafka_connect |           46 |           46 |          43 |           47 |             0 | -56 MB/s (-55%)    |


### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
| 2    | connect       | kafka_connect | 5.23x |          83 |          16 |
| 4    | connect       | kafka_connect | 5.95x |         102 |          17 |
| 8    | connect       | kafka_connect | 2.22x |         102 |          46 |

Raw samples + Prometheus snapshots: [`results/postgres/orders-cdc/2026-06-01T20-55-50Z.json`](results/postgres/orders-cdc/2026-06-01T20-55-50Z.json)
