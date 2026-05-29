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


## AWS — orders-cdc — 2026-05-21

**Scenario:** Stream changes from a 75M-row orders table under sustained heavy writes (target 80K writes/sec ≈ 96 MB/s).

**Git SHA:** [`deb26d3ad`](https://github.com/redpanda-data/connect/commit/deb26d3ade1275c44696d2c8cad954ea4d5d2cec)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.2xlarge` (400 GB) in `us-east-2`.

**Dataset:** 75,000,000 rows × 1200 B = ~83 GB

### Throughput

| GOMAXPROCS | MB/sec (p50) | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) |
|------------|--------------|-------------|--------------|---------------|
| 1          |           76 |          51 |           76 |        60,000 |
| 2          |           95 |          57 |           95 |        75,000 |
| 4          |           99 |          57 |          102 |        77,855 |

**Observations:**
- **1 vCPU is CPU-bound at ~76 MB/s** — the postgres_cdc read ceiling with 5K-message batches over 1.2KB rows.
- **2–4 vCPU is producer-bound at ~95–99 MB/s** — Connect has spare CPU at 2 vCPU and above; the load-gen + RDS write path caps at the 80K writes/sec ≈ 96 MB/s the workload was asked to push. A higher workload rate is required to expose Connect's multi-vCPU ceiling.
- **8 vCPU is omitted** — by sweep point 4 the `orders` table had grown to ~305M rows (3 × 17 min × ~75K writes/sec during the earlier points), and per-insert latency on the b-tree index stretched enough that the producer effectively stopped emitting. Connect spent the 8-vCPU window reading 0 msg/sec. A scenario revision should either truncate the table between sweep points or provision larger RDS storage so this doesn't reappear.

Raw samples + Prometheus snapshots: [`results/postgres/orders-cdc/2026-05-21T01-35-18Z.json`](results/postgres/orders-cdc/2026-05-21T01-35-18Z.json)


## AWS — orders-cdc — 2026-05-22

**Scenario:** Stream changes from a Postgres orders table under sustained heavy writes
(target 150K writes/sec ≈ 180 MB/s) so the postgres_cdc input — not the
producer — is the bottleneck across the whole CPU sweep. TRUNCATE between
sweep points keeps the table size bounded (no Trap 3).

**Git SHA:** [`a1454ba45`](https://github.com/redpanda-data/connect/commit/a1454ba45efa7f74ccac90620a1e2c82d41bbca3)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.2xlarge` (400 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | MB/sec (p50) | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) |
|------------|--------------|-------------|--------------|---------------|
| 1          |           57 |          25 |           76 |        45,000 |
| 2          |           64 |          19 |           95 |        50,000 |
| 4          |           64 |          19 |          102 |        50,000 |
| 8          |           70 |          25 |          102 |        55,000 |


Raw samples + Prometheus snapshots: [`results/postgres/orders-cdc/2026-05-22T00-09-28Z.json`](results/postgres/orders-cdc/2026-05-22T00-09-28Z.json)

**Observations:**

- **Medians came in LOWER than the 2026-05-21 run, but the cause is the producer, not Connect.** Bumping the workload to 150K writes/sec overwhelmed the 16-worker seeder pushing into a single Postgres table — sustained throughput collapsed to ~50K msg/sec with wild oscillation (5K → 107K minute-to-minute as RDS toggled between WAL-flush back-pressure and bursty release). Both medians and percentiles reflect producer noise, not Connect's behavior.
- **Connect's peak per sweep point is the honest signal here**: 83 → 102 → 108 → 108 MB/s. At 2+ vCPU `postgres_cdc` was instantaneously sinking the full producer output whenever the producer briefly recovered. **The peak `postgres_cdc` CDC throughput we measured on `c8g.4xlarge` is ~108 MB/s (~85K msg/sec).**
- **Connect was massively underutilized at all vCPU counts**: 74% / 57% / 39% / 21% of one core per vcpu (i.e. only 21% busy at 8 vCPU). Sub-100 MB heap, 30–36 goroutines, no anomalies. Whatever the real per-vCPU ceiling is, this scenario didn't get close.
- **For finding the real `postgres_cdc` per-vCPU CDC ceiling** the bottleneck must move off the producer: either bigger RDS instance class (`db.r6g.4xlarge`+), multi-table workload to split the WAL hotspot, or RDS Aurora. The single-table-on-2xlarge setup that worked well at 80K writes/sec doesn't survive 150K.
- **Mysql delivers ~50% more producer throughput on identical RDS** (~75K msg/sec sustained vs Postgres's ~50K) — see `mysql.md` 2026-05-22 run. Binlog (sequential append, no MVCC bloat) is a more permissive write workload than Postgres WAL for this kind of bench.


## AWS — orders-cdc-plan2-smoke — 2026-05-29

**Scenario:** Plan 2 smoke test — single vCPU, two engines (Connect + Kafka Connect)
sequentially. Verifies the engine-inner loop, KC REST submission, reset
extensions, and per-engine S3 artefacts. Not for publishable numbers.

**Git SHA:** [`79794a84c`](https://github.com/redpanda-data/connect/commit/79794a84c86349e68c2861b44ecac755550aad35)

**Infra:** Runner `c8g.xlarge`; source `db.r6g.large` (400 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           25 |            0 |          19 |           32 |        20,000 |                    |
| 1          | kafka_connect |           31 |           31 |          19 |           32 |             0 | +6 MB/s (+25%)     |


Raw samples + Prometheus snapshots: [`results/postgres/orders-cdc-plan2-smoke/2026-05-29T15-09-15Z.json`](results/postgres/orders-cdc-plan2-smoke/2026-05-29T15-09-15Z.json)
