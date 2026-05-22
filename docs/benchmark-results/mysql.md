# MySQL Benchmark Results

This file holds the AWS-bench results for `mysql_cdc`. Per-scenario rows are
appended by `task aws:bench` (see `benchmarking/aws/README.md`). For local
laptop-Docker bench results see [`mysql-cdc.md`](./mysql-cdc.md).

---


## AWS — orders-cdc — 2026-05-21

**Scenario:** Stream changes from a high-write MySQL orders table (target 80K writes/sec
≈ 96 MB/s) so the mysql_cdc input — not the producer — is the bottleneck.
TRUNCATE between sweep points keeps the table size bounded (no Trap 3).

**Git SHA:** [`a703be4fe`](https://github.com/redpanda-data/connect/commit/a703be4fef6300b0a4e0e1d47db137cb8bebc0ee)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.2xlarge` (400 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | MB/sec (p50) | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) |
|------------|--------------|-------------|--------------|---------------|
| 1          |          102 |          89 |          108 |        80,000 |
| 2          |          101 |          89 |          108 |        79,819 |
| 4          |          101 |          83 |          106 |        79,489 |
| 8          |           70 |          48 |          104 |        55,000 |


Raw samples + Prometheus snapshots: [`results/mysql/orders-cdc/2026-05-21T20-43-34Z.json`](results/mysql/orders-cdc/2026-05-21T20-43-34Z.json)

**Observations:**

- vCPU 1, 2, 4 are all pinned at the ~80K writes/sec × 1.2 KB ≈ 96 MB/s **producer cap** — Trap 1 from [bench-scenario-sizing-lessons](../superpowers/specs/2026-05-21-aws-benchmarking-mysql-design.md). `mysql_cdc`'s 1-vCPU CPU ceiling is at or above ~100 MB/s; at vCPU 1 the Connect process used 84% of a core (room for some, but not much), so the true ceiling is unlikely to be far above this.
- vCPU 8 dropped to 70 MB/s median with very wide variance (p5 48, p95 104). Per-second samples show a clean ~100 MB/s for the first ~3 minutes, then a sudden producer-side step-down to ~50K writes/sec (~60 MB/s) for the remaining 12 minutes. Connect itself had ample headroom throughout (12% of one core average, 30 goroutines, ~80 MB heap). The most likely cause is the RDS `backup_retention_period = 1` daily backup window coinciding with this sweep point's writes — gp3 throughput is shared between user writes and snapshot copy, and the timing (point 4 began roughly 90 minutes into the run) fits.
- **Follow-up:** retry with `backup_window` pinned outside the bench's typical run hours and `write_rate_per_sec: 150000` to find the real per-vCPU ceiling. Cost ~$3.


## AWS — orders-cdc — 2026-05-22

**Scenario:** Stream changes from a high-write MySQL orders table (target 150K writes/sec
≈ 180 MB/s) so the mysql_cdc input — not the producer — is the bottleneck
across the whole CPU sweep. TRUNCATE between sweep points keeps the table
size bounded (no Trap 3).

**Git SHA:** [`a1454ba45`](https://github.com/redpanda-data/connect/commit/a1454ba45efa7f74ccac90620a1e2c82d41bbca3)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.2xlarge` (400 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | MB/sec (p50) | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) |
|------------|--------------|-------------|--------------|---------------|
| 1          |           95 |          83 |          108 |        75,000 |
| 2          |           95 |          83 |          108 |        75,000 |
| 4          |           95 |          83 |          108 |        75,000 |
| 8          |           75 |          47 |          106 |        58,878 |


Raw samples + Prometheus snapshots: [`results/mysql/orders-cdc/2026-05-22T15-46-22Z.json`](results/mysql/orders-cdc/2026-05-22T15-46-22Z.json)

**Observations:**

- **vCPU 1/2/4 are tightly clustered at 95 MB/s p50 (75K msg/sec), p95 108 MB/s.** Despite bumping the workload target to 150K writes/sec, RDS MySQL on `db.r6g.2xlarge` only sustained ~75K writes/sec from the 16-worker seeder — half the requested rate. So all three points remain **producer-bound**, just at a 25% higher ceiling than the 80K-scenario run.
- **vCPU 1 used 76% of a core to deliver 95 MB/s** — close to but not at saturation. The true `mysql_cdc` 1-vCPU ceiling is probably in the 110–125 MB/s range (extrapolating from the peak of 121 MB/s).
- **vCPU 8 degraded again to 75 MB/s median** with the same shape as the 2026-05-21 run: clean ~95 MB/s for the first ~5 minutes, then a producer-side step-down to ~50K msg/sec for the rest of the window. The `backup_window = "06:00-08:00"` pin we added between runs did not prevent it (this run completed at 16:54 UTC, well past the backup window) — so the cause is NOT backup contention. More likely it's RDS-internal: gp3 throttling, binlog flush contention with WAL-equivalent secondary work, or a per-instance write-IOPS soft ceiling. Connect itself had 11%/vcpu CPU and sub-100 MB heap throughout — plenty of headroom.
- **Compared to postgres on identical RDS (`db.r6g.2xlarge`):** mysql sustained ~75K msg/sec as producer; postgres only sustained ~50K with much wider variance (5K–107K oscillation). MySQL's binlog (sequential append, no MVCC bloat) is a meaningfully better workload generator than Postgres WAL for this kind of bench.
- **For finding the real `mysql_cdc` per-vCPU CDC ceiling:** the bottleneck is the producer, not Connect. Next attempt should be one of: bigger RDS instance class (`db.r6g.4xlarge`+), multi-table workload (`tables: [orders_a..orders_d]` to split the write hotspot), or RDS Aurora (which routinely doubles single-instance write throughput vs vanilla RDS).
