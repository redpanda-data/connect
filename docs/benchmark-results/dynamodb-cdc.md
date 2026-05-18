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
