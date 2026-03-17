# Redpanda Migrator Benchmark Results

Benchmark suite: `internal/impl/redpanda/migrator/bench/`

## 2025-10-17 — Initial Benchmark

**PR:** https://github.com/redpanda-data/connect/pull/3696 (benchmark suite added alongside SQL Server CDC)

**Environment:**
- Docker Compose with CPU-pinned containers
- Source Redpanda: 1 CPU, 2.5GB RAM, epoll reactor, SMP=1
- Destination Redpanda: 1 CPU, 2.5GB RAM, epoll reactor, SMP=1
- Loader: 2 CPUs (GOMAXPROCS=2, GOMEMLIMIT=1GiB), 1.5GB RAM
- Migrator: 3 CPUs (GOMAXPROCS=3, GOMEMLIMIT=3GiB), 3.5GB RAM
- Topic: 40 partitions, write caching enabled, flush.ms=1000

**Dataset:** 30GB (30M messages, ~1KB each, uncompressed)

**Configuration:**
- `partition_buffer_bytes: 2MB`
- `max_yield_batch_bytes: 1MB`
- `max_in_flight: 40`

**Results:**

| Metric | Value |
|---|---|
| Throughput | 1.0 GB/sec |
| Messages/sec | ~1,035,000 |
| Total runtime | ~30 seconds |

```
[output.processors.0] msg="rolling stats: 1035873 msg/sec, 1.0 GB/sec"
[output.processors.0] msg="rolling stats: 1035211.5 msg/sec, 1.0 GB/sec"
[output.processors.0] msg="rolling stats: 1037427.5 msg/sec, 1.0 GB/sec"
```
