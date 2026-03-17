# SQL Server CDC Benchmark Results

Benchmark suite: `internal/impl/mssqlserver/bench/`

## 2025-10-17 — Michal's Run (Native ARM MSSQL)

**PR:** https://github.com/redpanda-data/connect/pull/3696

**Environment:**
- MSSQL running natively on ARM via Azure SQL Edge image (not x86 emulation)
- Colima VM: 4 vCPUs, 12GB RAM
- MSSQL free edition (limited to 4 CPUs)
- Single table CDC streaming

**Dataset:** 1.4KB x 21,198,489 rows = 24.1GB

**Results:**

| Metric | Value |
|---|---|
| Throughput | ~140-154 MB/sec |
| Messages/sec | ~105,000-119,000 |
| Peak | 154 MB/sec (119K msg/sec) |

```
INFO rolling stats: 116000 msg/sec, 150 MB/sec
INFO rolling stats: 119000 msg/sec, 154 MB/sec
INFO rolling stats: 113000 msg/sec, 146 MB/sec
INFO rolling stats: 114000 msg/sec, 147 MB/sec
INFO rolling stats: 105000 msg/sec, 136 MB/sec
```

**Observations:**
- SQL Server was melting 1 of 4 available cores (106% CPU)
- Only 1 of 100 configured connections was in use (`sql.DBStats{OpenConnections:1, InUse:1}`)
- Bottleneck is single-connection CDC reads, not Connect
- Theoretically 4x improvement possible by streaming multiple tables in parallel (proven at ~2x with 2 tables, 136% CPU)

### Multi-table test (2 tables, 2 connections)

```
INFO rolling stats: 76000 msg/sec, 102 MB/sec
INFO rolling stats: 69000 msg/sec, 92 MB/sec
INFO rolling stats: 74000 msg/sec, 99 MB/sec
```

SQL Server at 136% CPU with 2 connections active. Per-table throughput lower but total throughput scaled with connections.

## 2025-10-15 — Joe's Initial Run (Docker on Mac)

**PR:** https://github.com/redpanda-data/connect/pull/3696

**Environment:**
- macOS laptop
- SQL Server running in Docker (Azure SQL Edge, x86 emulation layer)
- Single table CDC snapshot

**Dataset:** 1.4KB x 21,198,489 rows = 24.1GB

**Results:**

| Metric | Value |
|---|---|
| Throughput | ~130-139 MB/sec |
| Messages/sec | ~65,000-105,000 |
| Total runtime | ~4m 30s |

```
INFO rolling stats: 81000 msg/sec, 108 MB/sec
INFO rolling stats: 101000 msg/sec, 135 MB/sec
INFO rolling stats: 104000 msg/sec, 139 MB/sec
INFO rolling stats: 99000 msg/sec, 133 MB/sec
INFO rolling stats: 105000 msg/sec, 141 MB/sec
INFO rolling stats: 103000 msg/sec, 138 MB/sec
```

**Observations:**
- Could not exceed ~139 MB/sec regardless of:
  - Increasing cores available to Connect
  - Running directly against SQL Server on a Linux VM
  - Running against various Azure SQL sizes (including Premium P1, 125 DTUs)
  - Using `sql_raw` input to rule out the CDC component
- Bottleneck is SQL Server's single-connection read throughput, not Connect
