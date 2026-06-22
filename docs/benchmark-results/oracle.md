# Oracle CDC Benchmark Results (AWS framework)

Suite: `benchmarking/aws/` — Connect `oracledb_cdc` vs Kafka Connect (Debezium
Oracle) on RDS Oracle, both mining the **same** redo logs via LogMiner so it's a
fair head-to-head. Scenario: `benchmarking/aws/scenarios/oracle/orders-cdc.yaml`
(impl committed in `0aa57241b`). Runner `c8g.4xlarge`; source `db.r5.2xlarge`
(800 GB, 24K IOPS) in `us-east-2`; 1200-byte rows; 150K rows/s write target.

## Summary — single-session LogMiner is the ceiling (2026-06-22)

**Headline:** Connect's `oracledb_cdc` streams Oracle CDC at **~13 MB/s on a
single LogMiner session, flat across CPU (1→8 vCPU) and flat across SCN-window
size (20K→200K).** The bottleneck is **LogMiner fetch on the database side** —
not Connect's CPU, and not the Oracle write path. The incumbent (Debezium) lands
in the same range, and an independent prior benchmark reached the same
conclusion, so this is a property of **Oracle LogMiner-based CDC**, not of
Connect.

### Connect vs Debezium across the CPU sweep — broker-side p50 (MB/s)

| vCPU | Connect (`oracledb_cdc`) | Kafka Connect (Debezium) |
|------|--------------------------|--------------------------|
| 1    | 13                       | 27 *(warm-up burst)*     |
| 2    | 13                       | 16                       |
| 4    | 13                       | 17                       |
| 8    | 13                       | 17                       |

Neither engine scales with CPU — both are bounded by single-session LogMiner
fetch. Debezium's high first point is a backlog/warm-up artifact that settles to
~16–17 MB/s. Connect is rock-steady at 13 (p5 6, p95 19) at every vCPU count.

### Why it's read-bound — three controls

1. **CPU has no effect.** Connect is identical at 1/2/4/8 vCPU → the reader is
   effectively single-threaded; extra cores don't help.
2. **The write path is not the limit.** A bulk seed with *no reader running*
   loaded 3,000,000 rows in 88 s = **~34K rows/s ≈ 41 MB/s**, ~3× the bench
   rate. Oracle ingests far faster than Connect reads.
3. **SCN-window size has no effect.** Raising `logminer.scn_window_size` 10×
   (20,000 → 200,000) left throughput unchanged (12 vs 13 MB/s). Larger mining
   windows do not lift the fetch ceiling.

### Corroboration

An independent local-Docker benchmark of `oracledb_cdc` (Joseph Woodward,
PR [#4082](https://github.com/redpanda-data/connect/pull/4082); see
[`oracledb-cdc.md`](./oracledb-cdc.md)) concluded: *LogMiner is single-threaded
by nature; the bottleneck is LogMiner fetch speed; Debezium shows similar
throughput → a protocol limitation, not a Connect limitation.* In byte terms the
two benches agree (~8–13 MB/s); that bench's higher msg/sec figure reflects
smaller rows (~167 B vs 1200 B here).

### Why there is no CPU-scaling curve (and why multi-table won't make one)

Oracle CDC has **no parallel change streams**. Every change in the database
flows through one shared redo log, read by one serial LogMiner session.
Splitting the dataset across multiple tables does **not** create independent
readers — all sessions mine the same redo and filter — so it cannot raise
per-source throughput, and it would not be representative anyway (real workloads
have large single tables).

This is the key contrast with the [DynamoDB CDC bench](./dynamodb-cdc.md), where
parallelism is **real and inherent**: DynamoDB Streams shard a single table and
the connector reads shards concurrently, so it scales to ~73 MB/s with CPU
(until source write-capacity bound). Oracle has no shard equivalent, so the
honest result is a fixed single-session ceiling, not a scaling curve.

### Practical takeaway

For Oracle CDC, a single `oracledb_cdc` input delivers ~13 MB/s regardless of
CPU or tuning, and is **on par with Debezium** — the ceiling is Oracle's
LogMiner protocol, not the tool. Higher aggregate throughput would require
multiple independent mining sessions (multiple connector instances / source
databases), which is an architectural choice, not a config knob.

### Reproduction notes (non-obvious scenario gotchas)

- **`stream_snapshot: true` is required even for an empty table.** With `false`
  + an empty checkpoint cache, the input starts from the *oldest* available redo
  SCN and never catches the live workload at low vCPU (0 MB/s, no error). `true`
  snapshots the empty table instantly and streams from `CURRENT_SCN`.
- **The reset step self-stages the seeder from S3 before TRUNCATE**, because
  reset runs on the runner host while the seeder is only staged on the load-gen
  host (Oracle has no psql/mysql CLI to do a network TRUNCATE).
- Raw per-run JSON + Prometheus snapshots are written to
  `benchmarking/aws/results/oracle/orders-cdc/` (not committed).
