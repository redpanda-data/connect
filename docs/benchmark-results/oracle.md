# Oracle CDC Benchmark Results (AWS framework)

Suite: `benchmarking/aws/` — Connect `oracledb_cdc` vs Kafka Connect (Debezium
Oracle) on RDS Oracle, both mining the **same** redo logs via LogMiner so it's a
fair head-to-head. Scenario: `benchmarking/aws/scenarios/oracle/orders-cdc.yaml`.
Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB, 24K IOPS) in `us-east-2`;
1200-byte rows; 150K rows/s write target.

Latest run: 2026-06-23 on `benchmarking` @ `2c2341b71` (after merging
`upstream/main`, which includes the recent `oracledb_cdc` performance work —
#4533 memory-efficient SQL scanner, #4531 LogMiner session/SCN handling, #4509
begin-at-current-SCN).

## Summary — LogMiner-bound, but the connector got faster (2026-06-23)

**Headline:** Connect's `oracledb_cdc` streams Oracle CDC at **~19 MB/s on a
single LogMiner session, flat across CPU (1→8 vCPU).** The bottleneck is still
**single-session LogMiner fetch** — adding cores does nothing — but the recent
upstream connector work lifted the absolute throughput **+46% (13 → 19 MB/s)**
by cutting per-record CPU/GC overhead (custom SQL scanner, better session
reuse). At steady state Connect now **matches-to-beats Debezium**.

### Connect vs Debezium across the CPU sweep — median MB/s

| vCPU | Connect (`oracledb_cdc`) | Kafka Connect (Debezium) |
|------|--------------------------|--------------------------|
| 1    | 19                       | 28 *(warm-up burst)*     |
| 2    | 19                       | 33 *(warm-up burst)*     |
| 4    | 19                       | 17                       |
| 8    | 19                       | 17                       |

Neither engine scales with CPU — both are bounded by single-session LogMiner
fetch. Connect is rock-steady at 19 (peak 32, p95 26) at every vCPU count.
**Debezium's sustained throughput is ~17 MB/s** — consistent across both the
2026-06-18 and 2026-06-23 runs at 4 and 8 vCPU. Its high 1–2 vCPU figures
(27–33) are **warm-up/backlog catch-up bursts and are not reproducible**: the
2 vCPU point measured 16 MB/s in the prior run and 33 here. They should *not* be
read as "Debezium is faster on fewer cores" (which would be nonsensical) — they
are an artifact of the median capturing an initial backlog-drain spike on the
first points of the sweep. **Compared on sustained throughput, Connect (19)
edges Debezium (~17).**

### Why it's read-bound, not write- or CPU-bound — the controls

1. **CPU has no effect.** Connect is identical at 1/2/4/8 vCPU → the reader is
   effectively single-threaded over one LogMiner session; extra cores don't
   help. (Same holds for Debezium.)
2. **The write path is not the limit.** A bulk seed with *no reader running*
   loaded 3,000,000 rows in 88 s = **~34K rows/s ≈ 41 MB/s**, ~2× the bench
   rate. Oracle ingests faster than either engine reads.
3. **Mining-window tuning doesn't lift it.** On the prior connector version,
   raising the LogMiner SCN window 10× (20K → 200K) left throughput unchanged;
   the merged code now includes adaptive SCN windowing (#4531) and reaches 19
   without manual tuning. The remaining ceiling is LogMiner fetch itself.

### What the upstream merge changed

- **+46% throughput (13 → 19 MB/s)** from connector efficiency — the
  allocation-heavy SQL parser was replaced with a custom memory-efficient
  scanner (#4533), and LogMiner session handling improved (#4531). The gain is
  lower per-record overhead, *not* parallelism — the curve is still flat.
- **#4509** made the connector begin at the database's current SCN on start.
  This natively fixes an earlier trap (with an empty checkpoint the connector
  used to start from the *oldest* available redo SCN and never catch the live
  workload at low vCPU → 0 MB/s). The scenario's `stream_snapshot: true` is no
  longer required for correctness, only retained for parity with Debezium.

### Corroboration

An independent local-Docker benchmark of `oracledb_cdc` (Joseph Woodward,
PR [#4082](https://github.com/redpanda-data/connect/pull/4082); see
[`oracledb-cdc.md`](./oracledb-cdc.md)) reached the same structural conclusion:
LogMiner is single-threaded; the bottleneck is LogMiner fetch speed; Debezium
shows similar throughput → a protocol limitation, not a Connect limitation.

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
honest result is a fixed single-session ceiling — now ~19 MB/s — not a scaling
curve.

### Practical takeaway

For Oracle CDC, a single `oracledb_cdc` input delivers ~19 MB/s regardless of
CPU, and is **on par with or ahead of Debezium** at steady state — the ceiling
is Oracle's LogMiner protocol, not the tool. Higher aggregate throughput would
require multiple independent mining sessions (multiple connector instances /
source databases), which is an architectural choice, not a config knob.

### Reproduction notes

- Run on `benchmarking` @ `2c2341b71`; both engines, `cpu_points: [1,2,4,8]`.
- The reset step self-stages the seeder from S3 before TRUNCATE, because reset
  runs on the runner host while the seeder is only staged on the load-gen host
  (Oracle has no psql/mysql CLI for a network TRUNCATE).
- Raw per-run JSON + Prometheus snapshots are written to
  `benchmarking/aws/results/oracle/orders-cdc/` (not committed).

---

### History

- **2026-06-23** (`2c2341b71`, post-upstream-merge): Connect **19 MB/s** flat;
  Debezium ~17 steady (bursts to 33). +46% vs prior code.
- **2026-06-18** (`63ea466c5`, pre-merge): Connect **13 MB/s** flat; Debezium
  ~16–27. Same single-session-LogMiner conclusion at a lower absolute number.
