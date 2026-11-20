# DynamoDB CDC Benchmark Results

Benchmark suite: `internal/impl/aws/dynamodb/bench/`

## 2026-03 — Initial Benchmark

**PR:** https://github.com/redpanda-data/connect/pull/4102

**Environment:**
- DynamoDB Local running in Docker (`amazon/dynamodb-local:latest`)
- In-memory mode (`-sharedDb -inMemory`)
- Local development machine

**Dataset:** 450,000 items (3 tables x 150,000 items)
- `bench-users`: 150K items with user profile data (~1KB each)
- `bench-products`: 150K items with product catalog data (~1KB each)
- `bench-orders`: 150K items with order data (~1KB each)

**Configuration:**
- `batch_size: 1000`
- 3 tables monitored via DynamoDB Streams

**Results:**

| Metric | Value |
|---|---|
| Throughput | ~200-216 MB/sec |
| Messages/sec | ~95,000-102,000 |

```
INFO rolling stats: 99000 msg/sec, 204 MB/sec
INFO rolling stats: 95516 msg/sec, 198 MB/sec
INFO rolling stats: 102000 msg/sec, 216 MB/sec
```

**Observations:**
- DynamoDB Local uses a single shard per table — with 3 tables the connector fully saturates each shard
- After all records are consumed, throughput drops to 0 until new writes arrive
- Real AWS DynamoDB scales horizontally with multiple shards per table, so production throughput could be higher
- DynamoDB Streams retain records for only 24 hours — seed data and run the benchmark promptly

## 2026-11 — Incremental snapshot with concurrent writes

**PR:** https://github.com/redpanda-data/connect/pull/4886

**Benchmark:** `BenchmarkIntegrationDynamoDBCDCIncrementalSnapshot` in `internal/impl/aws/dynamodb/input_cdc_incremental_bench_test.go`

**Environment:**
- DynamoDB Local running in Docker
- Apple M4 Pro
- One run per case

**What it measures:** a backfill of one table, run with and without writers that update random rows of the table while those rows are being read. Every write is `ADD v 1`, so a key's value only grows. The benchmark counts:

- **Safety violations:** a READ emitted with a lower `v` than one already emitted for that key, that is, a stale snapshot item that overtook a newer change.
- **Order regressions:** any event with a lower `v` than the previous event for its key.
- **Final mismatches:** keys whose last emitted value differs from a consistent Scan taken after the stream goes quiet.
- **MODIFY latency:** time from each write to its MODIFY leaving the input, while the backfill runs.

**Results:**

| Mode | Rows | Segments × batch | Writes during backfill | Backfill | READ items/s | READs dropped | MODIFY p50 / p95 | Safety violations | Final mismatches |
|---|---|---|---|---|---|---|---|---|---|
| `incremental` (defaults) | 1,000 | 1 × 100 | none | 106.8 s | 9.4 | 0 | - | 0 | 0 |
| `incremental` | 10,000 | 10 × 1000 | none | 67.4 s | 148 | 0 | - | 0 | 0 |
| `incremental` | 10,000 | 10 × 1000 | 50/s (3,276 writes) | 65.6 s | 148 | 271 | 497 / 951 ms | 0 | 0 |
| `incremental` | 10,000 | 10 × 1000 | 200/s (21,648 writes) | 108.3 s | 35 | 6,166 | 500 / 956 ms | 0 | 0 |
| `snapshot_and_cdc` | 10,000 | 10 × 1000 | none | 1.1 s | 8,803 | - | - | 0 | 0 |
| `snapshot_and_cdc` | 10,000 | 10 × 1000 | 200/s (220 writes) | 1.2 s | 8,652 | - | none delivered | 0 | 220 (see below) |

No case had an order regression.

**Observations:**
- **Writes to the rows being read.** `incremental` emitted no stale snapshot item, and every key ended on the table's value. A row that changes while its page is held is dropped from the page and arrives through the stream instead ("READs dropped"). At 200 writes/s, 62% of the rows came from the stream.
- **Streaming is not held back by the backfill.** MODIFY events left the input within 0.5 s (p50) and 1 s (p95), which is the default 1 s `poll_interval`.
- **Backfill speed.** `incremental` is slower than `snapshot_and_cdc` by design, because each segment holds one page until the stream has caught up past it. Two DynamoDB Local effects make the local numbers slower than AWS would be:
  - A shard that has never returned a record holds pages for `idle_shard_grace` (1m). The benchmark enables the stream just before the run, so the cases without writes pay that minute on the first page. That is most of the 67 s for 10,000 rows.
  - DynamoDB Local rounds `ApproximateCreationDateTime` down to the minute. While writes continue, a page is released only once a record from the next minute arrives, so at 200 writes/s the backfill took two of those rounds (108 s). On AWS the timestamps are accurate to the second, and a page waits about 1 s + `watermark_margin` (2 s by default). Not yet measured on AWS.
- **`snapshot_and_cdc` under writes is not a correctness comparison on DynamoDB Local.** Its dedupe skips stream events stamped before the snapshot start, and the minute rounding makes all 220 changes look older, so none were delivered. Only its throughput is comparable here.

**To run it:**

```bash
go test -run BenchmarkIntegrationDynamoDBCDCIncrementalSnapshot -bench BenchmarkIntegrationDynamoDBCDCIncrementalSnapshot -benchtime 1x -timeout 60m ./internal/impl/aws/dynamodb/
```
