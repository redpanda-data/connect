# Oracle CDC Benchmark Results

Benchmark suite: `internal/impl/oracledb/bench/`

## 2026-03 — Initial Benchmark

**PR:** https://github.com/redpanda-data/connect/pull/4082

**Environment:**
- Oracle Database Express Edition (XE) 21c running in Docker
- Container image: `container-registry.oracle.com/database/express:latest`

**Dataset:** 18,000,000 rows, ~3GB

```sql
SELECT segment_name, bytes / 1024 / 1024 AS mb FROM dba_segments WHERE owner = 'TESTDB';

SEGMENT_NAME         MB
_______________ _______
USERS              2950
SYS_C008322         288
```

**Configuration:**
- `snapshot_max_batch_size: 160000`
- `logminer.scn_window_size: 190000`
- `batching.count: 140000`

### Snapshot Results

| Metric | Value |
|---|---|
| Messages/sec | ~140,000 |

Snapshot mode benefits from greater concurrency and behaves similarly across SQL-based connectors.

### LogMiner Streaming Results

| Metric | Value |
|---|---|
| Total runtime | ~12 minutes |
| Average throughput | ~50,000 msg/sec (total average: 25,000 msg/sec) |
| Peak throughput | ~70,000-90,000 msg/sec |

**Observations:**
- LogMiner was never designed for CDC and has inherent performance limitations:
  - Requires buffering transactions until `COMMIT` or `ROLLBACK` before flushing
  - Large gaps between redo log SCNs before reaching relevant data
  - Single-threaded by nature
- Removing the buffering layer improved things slightly but not significantly, confirming the bottleneck is LogMiner fetch speed
- Quick comparison with Debezium's Oracle CDC (also LogMiner-based) showed similar throughput, confirming this is a protocol limitation, not a Connect limitation
