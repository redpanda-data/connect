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
