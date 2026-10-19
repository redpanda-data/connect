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

---

## CDC / Streaming — reader microbenchmark (2026-09-17, [#4812](https://github.com/redpanda-data/connect/pull/4812))

**Environment:** Apple M4 Pro (14 cores), 48 GB RAM, macOS 26.2, go1.26.6 darwin/arm64. No Docker: `BenchmarkStreamMessages` drives `Stream.streamMessages` against an in-process fake walsender over TCP loopback, with a consumer goroutine that only counts rows.

**Dataset:** 200,000 synthetic `orders` inserts of ~1.2 KB WAL each (1,100 B text payload), 100 rows per transaction, pgoutput `proto_version 1` text tuples.

**Configuration:** reader only, default caps (1000 rows / 4 MiB of WAL per streaming batch, channel depth 4). The input's consumer stage (JSON marshal, metadata, batcher, checkpoint) is not in the loop, so ns/row is the reader plus the fake sender's pgproto3 encoding and loopback syscalls, not a pure decode cost.

```
go test ./internal/impl/postgresql/pglogicalstream/ -run '^$' -bench BenchmarkStreamMessages -benchmem -benchtime 200000x -count 3
```

| Tree | ns/row | MB/s | B/row | allocs/row |
|---|---|---|---|---|
| before: main plus the benchmark only (38060f7ec) | 3,485-3,564 | 326-333 | 10,760 | 59 |
| after: PR #4812 head | 1,952-2,024 | 574-595 | 10,225 | 48 |

Raw output, before:

```
BenchmarkStreamMessages-14    200000    3485 ns/op    333.42 MB/s    10760 B/op    59 allocs/op
BenchmarkStreamMessages-14    200000    3520 ns/op    330.13 MB/s    10758 B/op    59 allocs/op
BenchmarkStreamMessages-14    200000    3564 ns/op    326.05 MB/s    10757 B/op    59 allocs/op
```

Raw output, after (5 runs):

```
BenchmarkStreamMessages-14    200000    1952 ns/op    595.20 MB/s    10224 B/op    48 allocs/op
BenchmarkStreamMessages-14    200000    2012 ns/op    577.65 MB/s    10225 B/op    48 allocs/op
BenchmarkStreamMessages-14    200000    2004 ns/op    579.71 MB/s    10225 B/op    48 allocs/op
BenchmarkStreamMessages-14    200000    2024 ns/op    574.08 MB/s    10225 B/op    48 allocs/op
BenchmarkStreamMessages-14    200000    2022 ns/op    574.61 MB/s    10224 B/op    48 allocs/op
```

**Observations:**
- About 1.75× on the reader path: one channel send per transaction instead of per row, one receive-deadline context per standby cycle instead of per row, one hard-stop context per stream instead of per WAL frame.
- The gain is a lower per-row reader cost. The reader/consumer overlap the buffered channel enables is not visible here because the consumer is a counter; it shows up only in the end-to-end runs below.
- With no `batching` policy and `batch_transactions` off (the default), the input's consumer still emits one message per row, so end-to-end throughput in that configuration is bounded by the consumer stage rather than the reader.

**Pending (real-endpoint phase):** the Docker harness in [`internal/impl/postgresql/bench/`](../../internal/impl/postgresql/bench/) (cart table, GOMAXPROCS 1/2/4/8) for the default configuration, `batch_transactions: true`, and `batching.count: 1000`, plus a rerun of the AWS orders-cdc scenario. Both need an enterprise license on the bench host; results will be appended here as a further dated section when run.
