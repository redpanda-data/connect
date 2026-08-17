# MySQL Benchmark Results

AWS bench results for `mysql_cdc`. Per-scenario sections are appended by `task aws:bench` (see `benchmarking/aws/README.md`). For local laptop-Docker mysql results see [`mysql-cdc.md`](./mysql-cdc.md).

---

## AWS — orders-cdc — 2026-06-02

**Scenario:** Stream changes from a high-write MySQL orders table (target 150K writes/sec
≈ 180 MB/s) so the mysql_cdc input — not the producer — is the bottleneck
across the whole CPU sweep. TRUNCATE between sweep points keeps the table
size bounded (no Trap 3).

**Git SHA:** [`25057d693`](https://github.com/redpanda-data/connect/commit/25057d6936c7785ca918aa09eac8a1341afcf875)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.4xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           70 |            4 |          64 |           75 |        55,000 |                    |
| 1          | kafka_connect |           33 |           33 |          31 |           35 |             0 | -37 MB/s (-53%)    |
| 2          | connect       |          102 |            6 |          95 |          108 |        80,000 |                    |
| 2          | kafka_connect |           35 |           35 |          32 |           36 |             0 | -67 MB/s (-65%)    |
| 4          | connect       |          108 |            6 |         104 |          114 |        85,000 |                    |
| 4          | kafka_connect |           54 |           54 |          50 |           54 |             0 | -54 MB/s (-50%)    |
| 8          | connect       |          111 |            7 |         105 |          114 |        87,167 |                    |
| 8          | kafka_connect |           39 |           39 |          34 |           43 |             0 | -72 MB/s (-65%)    |


### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
| 1    | connect       | kafka_connect | 2.14x |          70 |          33 |
| 2    | connect       | kafka_connect | 2.90x |         102 |          35 |
| 4    | connect       | kafka_connect | 2.02x |         108 |          54 |
| 8    | connect       | kafka_connect | 2.88x |         111 |          39 |

Raw samples + Prometheus snapshots: [`results/mysql/orders-cdc/2026-06-02T14-13-52Z.json`](results/mysql/orders-cdc/2026-06-02T14-13-52Z.json)


## AWS — orders-snapshot — 2026-08-17

**Scenario:** Snapshot a pre-seeded 30M-row (36 GB logical) InnoDB orders table via
mysql_cdc stream_snapshot, A/B-ing the default snapshot_max_batch_size=1000
against 50000. Buffer pool pinned to 16 GiB so the scan is disk-bound and
the oracle cold-amplification comparison is honest. Bounded-dataset mode:
no workload, warmup 0, snapshot visible from t=0.

**Git SHA:** [`54eb40c23`](https://github.com/redpanda-data/connect/commit/54eb40c23bdb9ebcb8bb5105d97c6fb19d53289c)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.4xlarge` (800 GB) in `us-east-2`.

**Dataset:** 30,000,000 rows × 1200 B = ~33 GB

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 8          | s0-batch-default | connect       |           21 |       21.191 |        17,406 |           21 |          20 |           23 |        17,500 |                    |
| 4    | 8          | s1-batch-50000 | connect       |           22 |       21.842 |        17,940 |           22 |          21 |           23 |        18,000 |                    |


Raw samples + Prometheus snapshots: [`results/mysql/orders-snapshot/2026-08-17T21-18-26Z.json`](results/mysql/orders-snapshot/2026-08-17T21-18-26Z.json)

### Findings — the Oracle PK-ordering pathology does NOT reproduce on MySQL

Context: on Oracle (`oracle-logminer-split-test.md`, snapshot sections), keyset
pagination in PK order over a PK-scattered heap caused ~12x physical read
amplification and a cold-rate collapse to ~9 MB/s; removing the ORDER BY fixed
it. `mysql_cdc` runs the same query shape (`snapshot.go::querySnapshotTable`:
`WHERE (pk) > (last) ORDER BY pk LIMIT n`), so this run tested whether MySQL
pays the same cost. It does not — InnoDB tables are clustered on the PK, so PK
order IS physical order and the ordering is free.

Evidence (RDS CloudWatch, 5-min averages over each arm's window):

| arm | delivered MB/s | physical ReadThroughput | amplification | ReadIOPS | DB CPU |
|-----|----------------|-------------------------|---------------|----------|--------|
| s0-batch-default | 21.3 | ~24.8 MB/s | **~1.16x** | ~1,200–1,700 (of 24,000) | ~1.4% |
| s1-batch-50000   | 21.9 | ~25.7 MB/s | **~1.17x** | ~1,200–1,700 | ~1.4% |

The scan was genuinely disk-backed: the buffer pool was pinned to 16 GiB
against 36 GB of data, and physical reads tracked the delivered rate for the
whole window in both arms. Per-minute means are flat (s0: 20.0–22.5, s1:
21.8–23.4) — no cold collapse, no warm ramp. Compare Oracle's 12x under the
same query shape.

Second result: **`snapshot_max_batch_size` is not a lever.** 1000 (default,
= 30,000 keyset queries for the table) vs 50,000 (600 queries) moved the rate
+3% (21.3 → 21.9 MB/s). On a clustered PK each batch re-seek is a cheap B-tree
descend, so cutting round trips 50x buys almost nothing — consistent with the
ordering-is-free story, and unlike Oracle where the equivalent lever
(PREFETCH_ROWS) was worth 4–8x.

Third result (new question): **the ~21 MB/s snapshot ceiling is Connect-side,
not database-side.** The DB sat at ~1.4% CPU with disk at ~7% of provisioned
IOPS; both arms pinned at ~17.5–18K rows/s (~57 µs/row) regardless of batch
size. For scale, on this same rig mysql_cdc binlog *streaming* does 111 MB/s,
and Oracle's fixed snapshot fetch path did 98 MB/s warm. The suspect is the
single-threaded per-row snapshot path (scan → per-value mappers → per-row map
alloc → single channel); `max_parallel_snapshot_tables` cannot help a
single-table snapshot. A 36 GB table extrapolates to ~29 min at this rate.

Caveats: the seeded table has an AUTO_INCREMENT PK (densely packed, built in
PK order) — representative of typical InnoDB usage, but not a scatter-seeded
worst case; secondary-index-heavy or UUID-PK tables would fragment leaf pages
and could raise the amplification somewhat. Insert-only dataset, single table,
one instance class (db.r6g.4xlarge, gp3 24,000 IOPS).
