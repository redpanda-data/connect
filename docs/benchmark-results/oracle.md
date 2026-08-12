# Oracle CDC Benchmark Results (AWS framework)

> **⚠ Every result dated before 2026-08-05 in this file is invalid — do not quote it.**
> The load generator gated blocking inserts on a 100 ms `time.Ticker`. Go discards missed
> ticks, so a worker whose insert took ~1.5 s achieved ~0.67 inserts/sec instead of 10,
> capping delivered load near **12 MB/s regardless of the requested rate**. The
> "~13 MB/s flat across 1-8 vCPU" and "single-session LogMiner ceiling" conclusions below
> therefore measured *the load generator*, not Oracle: Connect was keeping pace with a load
> that was identical at every core point. Fixed 2026-08-05 in
> `benchmarking/aws/seeders/cdc-rows-oracle/sql.go` (continuous pacing + per-table
> delivered-rate logging).
>
> **For current findings see [`oracle-logminer-split-test.md`](oracle-logminer-split-test.md).**
> Headline: the ceiling is **per reader**, not per database — ~7-12 MB/s sustained per reader,
> ~19 MB/s draining a backlog, plateauing near ~31 MB/s per database; and cores are not the
> constraint (Connect never exceeded 1.0 of 4).

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


## AWS — orders-5table-split — 2026-08-05

**Scenario:** DISCRIMINATOR TEST — is the oracledb_cdc throughput limit the traversal of
Oracle's single ordered redo stream, or the per-session fetch/parse of the
rows that match a session's table filter?

This question was never answerable from the earlier oracle/orders-cdc runs.
Those runs' workload generator was ticker-gated and silently capped near
~10K rows/sec (~12 MB/s) regardless of the requested rate, so "Connect flat
at 13 (later 19) MB/s across 1-8 vCPU" is equally consistent with "Connect
simply kept pace with a load that was identical at every point". The seeder's
workload path is now a continuous paced loop (mirroring bulkInsert, which
sustained ~41 MB/s on this instance class) and logs delivered rows/sec and
MB/s per table every 10s, so the read-side number can be read against the
load that actually arrived.

Load: 5 tables, ~7 MB/s of row data each, ~35 MB/s total. Note that redo
volume is materially HIGHER than 35 MB/s: every table carries ADD
SUPPLEMENTAL LOG DATA (ALL) COLUMNS, so redo also holds full before-images
plus undo and index maintenance. That is deliberate — it matches what a
customer running Debezium-style CDC actually generates.

Arms (identical except for the input's include list):
  a-1table  — one input mining ONE of the 5 loaded tables.
  b-5tables — one input mining ALL 5.

Reading the result:
  a ~= 7 and b ~= 35  -> no session ceiling near 19 at all; the old number was
                         load-bound. One input serves all 5 tables; no split.
  a ~= 7 and b ~= 19-25 -> a session CAN traverse a ~35 MB/s stream while
                         emitting only its own table, so the limit is
                         per-session fetch/parse of matched rows. Splitting
                         into N inputs raises aggregate throughput.
  a << 7              -> the session cannot even keep pace with the stream
                         while emitting one table's worth. Traversal-bound:
                         splitting buys nothing and ~19 MB/s is the real
                         aggregate ceiling for LogMiner.

Cores are deliberately generous (4 vCPU, GOMAXPROCS 8, both arms) so that a
shortfall cannot be mistaken for CPU starvation. This is a shape test, not a
CPU sweep — cpu_points stays at a single value on purpose.

**Git SHA:** [`f1ccf5289`](https://github.com/redpanda-data/connect/commit/f1ccf52899ab019a9fc93ae638e1741357e4825b)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 8          | a-1table       | connect       |            6 |        7.719 |         6,014 |            0 |           3 |           13 |         5,000 |                    |
| 4    | 8          | b-5tables      | connect       |           19 |       21.329 |        16,866 |            2 |          13 |           32 |        15,000 |                    |


Raw samples + Prometheus snapshots: [`results/oracle/orders-5table-split/2026-08-05T20-48-45Z.json`](results/oracle/orders-5table-split/2026-08-05T20-48-45Z.json)


## AWS — orders-5table-readers — 2026-08-06

**Scenario:** FOLLOW-UP TO oracle/orders-5table-split — does adding READERS raise aggregate
throughput, and what does it cost the database?

orders-5table-split established that the oracledb_cdc ceiling is per-reader,
not per-database: one input mining 1 of 5 loaded tables captured 98% of its
table (and went idle 2.7% of samples) while one input mining all 5 captured
only 55% and never idled. It could not answer the next question, because only
ONE reader was ever active: every reader runs its own independent LogMiner
mining pass over the whole redo stream, so N readers may cost the DATABASE up
to N times the mining work even though each reader only parses its own rows.

This scenario sweeps reader COUNT at fixed load: the same 5 tables written at
~7 MB/s each (~35 MB/s total) in every arm, with the 5 tables distributed over
1, then 2, then 5 concurrent `oracledb_cdc` inputs inside a `broker` input
(benthos `broker` reads its inputs in parallel, so this is genuinely N
concurrent LogMiner sessions in one process). Each reader gets its OWN
checkpoint_cache_key — they share one memory cache resource, and the default
key would make them clobber each other's SCN.

Reading the result — aggregate captured throughput:
  r1 ~19-21, r2 ~35, r5 ~35  -> readers scale, 5 x 7 MB/s is achievable.
  r1 ~19-21, r2 ~30, r5 ~30  -> partial scaling; a plateau to characterise.
  r1 ~ r2 ~ r5 ~ 19-21       -> the ceiling is shared after all, and the
                                split-test result does NOT generalise to
                                concurrent readers.

AND THE MEASUREMENT THIS SCENARIO EXISTS FOR — Oracle's own CPU. The harness
does not scrape source-database metrics, so collect it post-run from
CloudWatch per arm window (the RDS identifier `rpcn-bench-ora-ora` is stable
and metrics outlive teardown):
  CPU flat across r1/r2/r5   -> Oracle pushes the table filter down and skips
                                non-matching redo cheaply; many readers are
                                affordable.
  CPU scaling ~linearly      -> each reader really does re-mine the whole
                                stream; readers per database are tightly
                                capped and the ~24-tables-per-DB plan needs
                                few, larger readers.

Load is held IDENTICAL across arms so aggregate capture % is comparable, and
cores stay at 4 vCPU / GOMAXPROCS 8 (as in orders-5table-split) so a change
cannot be a CPU artifact — measured per-reader cost there was only ~0.26
cores, so 5 readers should need ~1.3.

**Git SHA:** [`f1ccf5289`](https://github.com/redpanda-data/connect/commit/f1ccf52899ab019a9fc93ae638e1741357e4825b)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 8          | r1-one-reader  | connect       |           19 |       18.969 |        14,949 |            2 |          13 |           32 |        15,000 |                    |
| 4    | 8          | r2-two-readers | connect       |           25 |       25.912 |        20,469 |            2 |          17 |           38 |        20,000 |                    |
| 4    | 8          | r5-five-readers | connect       |           29 |       30.669 |        24,090 |            2 |          21 |           44 |        23,085 |                    |


Raw samples + Prometheus snapshots: [`results/oracle/orders-5table-readers/2026-08-06T15-02-03Z.json`](results/oracle/orders-5table-readers/2026-08-06T15-02-03Z.json)


## AWS — orders-5table-baseline — 2026-08-06

**Scenario:** DECOMPOSITION RUN — splits Oracle-side CPU into write cost, stream-mining
cost, and matched-row-return cost.

orders-5table-readers measured Oracle CPU at 45.9% / 50.5% / 72.2% for 1 / 2 /
5 concurrent readers, refuting the pessimistic "N readers = N x mining work"
bound (5x readers cost only ~1.57x CPU) but leaving the numbers
uninterpretable in absolute terms: the 35 MB/s write workload consumes Oracle
CPU too, and there was no baseline to subtract. Aggregate throughput also
plateaued at 30.7 MB/s (79% of offered) with Oracle at 72%, and it was unclear
how much of that ceiling is duplicated mining versus row return.

Three arms, identical 35 MB/s write load, all on ONE RDS instance so the CPU
figures are directly differencable (cross-run CPU comparison is not reliable):

  w0-writes-only   no Oracle reader at all (a trivial `generate` input keeps
                   the pipeline valid). Oracle CPU here is PURE WRITE COST.
  m1-empty-reader  one oracledb_cdc reader whose include matches
                   BENCH.ORDERS_IDLE — a table that exists, carries
                   supplemental logging, and is NEVER written. The session
                   opens, mines the full redo stream, and returns ZERO rows.
  r1-one-reader    one reader over all five written tables. Reproduces
                   orders-5table-readers' r1 (18.97 MB/s mean, 45.9% Oracle
                   CPU) as an in-run reference point.

The decomposition:
  W          = w0 CPU                  -> write path
  M - W      = m1 CPU - w0 CPU         -> ONE session mining the whole stream,
                                          returning nothing
  R1 - M     = r1 CPU - m1 CPU         -> reconstructing and returning five
                                          tables' matched rows

Why it matters: if (M - W) is large, each additional reader pays a big fixed
mining toll and reader count is tightly capped — few, large readers. If
(M - W) is small and (R1 - M) dominates, mining is cheap, duplication is
nearly free, and the 5-reader plateau must be explained by something else
(row-return contention, or Oracle CPU saturation near 72%).

ARM ORDER IS DELIBERATE: w0 runs LAST. It is the novel shape (a source
scenario whose input never touches the source), so if it trips a harness
assumption about zero-throughput points, the two informative arms have
already completed.

**Git SHA:** [`f1ccf5289`](https://github.com/redpanda-data/connect/commit/f1ccf52899ab019a9fc93ae638e1741357e4825b)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 8          | m1-empty-reader | connect       |            0 |        0.000 |             0 |            0 |           0 |            0 |             0 |                    |
| 4    | 8          | r1-one-reader  | connect       |           19 |       20.148 |        15,908 |            2 |          13 |           32 |        15,000 |                    |
| 4    | 8          | w0-writes-only | connect       |            0 |        0.000 |             0 |            0 |           0 |            0 |             0 |                    |


Raw samples + Prometheus snapshots: [`results/oracle/orders-5table-baseline/2026-08-06T16-27-35Z.json`](results/oracle/orders-5table-baseline/2026-08-06T16-27-35Z.json)


## AWS — orders-return-cost — 2026-08-06

**Scenario:** FINAL DECOMPOSITION — the clean cost of RETURNING matched rows, measured
between two readers that both keep up with the stream.

orders-5table-baseline produced W=34.4%, M=46.4% (a reader mining everything,
returning no DML) and R1=42.2% (a reader over all 5 tables). R1 - M came out
NEGATIVE (-4.3 points), which is not a negative cost: r1 captured only ~54% of
the load, so in a fixed window it advanced through roughly half as much redo as
m1 did and therefore performed LESS mining. The two arms did not mine the same
amount of stream, so the subtraction was invalid.

This run fixes that by keeping every reader arm UNDER the ~19 MB/s
single-reader ceiling, so all of them keep pace with the stream and mining
coverage is identical by construction:

  w0-writes-only   no Oracle reader. Oracle CPU = write path (control).
  m0-zero-tables   one reader on BENCH.ORDERS_IDLE (never written). Mines the
                   whole stream, returns no DML. NOTE it still receives
                   transaction-control rows (START/COMMIT/ROLLBACK are matched
                   unfiltered, because transaction boundaries are needed
                   regardless of table), so this is "scan + txn bookkeeping",
                   not "scan alone".
  a2-two-tables    one reader on ORDERS_T1 + ORDERS_T2 = 14.0 MB/s returned,
                   comfortably under the ~19 MB/s ceiling so it keeps up.

The measurement:
  A2 - M0  = cost to Oracle of reconstructing and shipping 14 MB/s of matched
             rows, with mining held constant. THIS is the number that was
             previously unobtainable.
  M0 - W   = the per-reader scan + txn-bookkeeping toll (re-measured in-run;
             was 12.0 points).

VALIDITY GATE: a2 must capture ~100% of its two tables (~12,232 rows/s). If it
captures materially less it fell behind, mining coverage diverged again, and
A2 - M0 is invalid exactly as R1 - M was. Check this before trusting the
subtraction.

SENSITIVITY: the effect being measured is small (previous indirect estimate was
1-2 points) against per-minute CPU noise of roughly +/-8 points. Hence 30-minute
windows rather than 15, doubling CloudWatch samples per arm to ~30, and only
three arms so total wall clock stays near 2h — well inside the 4h
orphan-cleanup TTL. Even so, treat a result under ~2 points as "smaller than we
can resolve", not as zero.

Write load is held at 5 tables x 7 MB/s = 35 MB/s, identical to
orders-5table-split / -readers / -baseline, so W and the scan toll stay
comparable with those runs.

**Git SHA:** [`f1ccf5289`](https://github.com/redpanda-data/connect/commit/f1ccf52899ab019a9fc93ae638e1741357e4825b)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 8          | m0-zero-tables | connect       |            0 |        0.000 |             0 |            0 |           0 |            0 |             0 |                    |
| 4    | 8          | a2-two-tables  | connect       |           13 |       13.303 |        10,340 |            1 |           6 |           19 |        10,000 |                    |
| 4    | 8          | w0-writes-only | connect       |            0 |        0.000 |             0 |            0 |           0 |            0 |             0 |                    |


Raw samples + Prometheus snapshots: [`results/oracle/orders-return-cost/2026-08-06T19-27-01Z.json`](results/oracle/orders-return-cost/2026-08-06T19-27-01Z.json)


## AWS — orders-5table-readers-fastio — 2026-08-06

**Scenario:** WHY DOES THE 5-READER PLATEAU HAPPEN? Repeat of orders-5table-readers with the
storage throughput ceiling removed.

orders-5table-readers found 1/2/5 concurrent readers delivering 19.0 / 25.9 /
30.7 MB/s against a 35 MB/s offered load — sub-linear, plateauing at 79%
capture. Neither side was exhausted: database CPU 72% of 8 vCPU, Connect 0.99
of 4 cores. CloudWatch I/O metrics from that run explain it:

  total IOPS       11,149 -> 10,726 -> 13,313   (55% of 24,000 provisioned)
  total throughput    340 ->    320 ->    452 MiB/s
  DiskQueueDepth     26.4 ->   26.5 ->   34.2

~452 MiB/s is 90% of RDS's ~500 MiB/s gp3 DEFAULT throughput (never raised,
because `storage_throughput` was set nowhere in the Terraform) and 76% of a
db.r5.2xlarge's ~594 MiB/s EBS bandwidth. The clincher: WriteIOPS FELL as
readers were added (8,405 -> 7,212 -> 7,158) while total I/O stayed pinned —
the readers were taking I/O away from the writers. Classic saturation.

This scenario raises both ceilings and re-runs the identical 1/2/5 reader arms.
Reading the result:
  5 readers well above 30.7 MB/s -> the plateau was the test rig's storage, and
      the ~31 MB/s "per-database ceiling" in
      docs/benchmark-results/oracle-logminer-split-test.md is a property of
      db.r5.2xlarge on default gp3 throughput, NOT of LogMiner. Guidance must
      then be restated against provisioned I/O.
  5 readers near 30.7 again -> storage was NOT the cause despite the saturation
      signature, and the plateau is something else (redo latch contention, or a
      serialised component inside LogMiner).

Everything except `infra.source` is identical to orders-5table-readers, so the
two runs are directly comparable.

**Git SHA:** [`3e8bf51d4`](https://github.com/redpanda-data/connect/commit/3e8bf51d4154084e2fb8498addaf86dfc9827faa)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.4xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 8          | r1-one-reader  | connect       |           19 |       18.625 |        14,692 |            2 |          13 |           25 |        15,000 |                    |
| 4    | 8          | r2-two-readers | connect       |           25 |       24.007 |        19,002 |            2 |          17 |           32 |        20,000 |                    |
| 4    | 8          | r5-five-readers | connect       |           30 |       31.059 |        24,392 |            2 |          24 |           43 |        23,304 |                    |


Raw samples + Prometheus snapshots: [`results/oracle/orders-5table-readers-fastio/2026-08-06T22-17-54Z.json`](results/oracle/orders-5table-readers-fastio/2026-08-06T22-17-54Z.json)


## AWS — orders-snapshot — 2026-08-12

**Scenario:** Snapshot a pre-seeded 30M-row (36 GB logical) Oracle orders table via
oracledb_cdc stream_snapshot, A/B-ing go-ora's default 25-row prefetch
against PREFETCH_ROWS=1000. Bounded-dataset mode: no workload, warmup 0,
snapshot visible from t=0.

**Git SHA:** [`b115f77c6`](https://github.com/redpanda-data/connect/commit/b115f77c67ed40a6336ce3f28da32fd951e8094a)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 30,000,000 rows × 1200 B = ~33 GB

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 8          | s0-prefetch-default | connect       |            7 |        7.256 |         6,213 |            7 |           7 |            8 |         6,000 |                    |
| 4    | 8          | s1-prefetch-1000 | connect       |            9 |       14.866 |        12,726 |            9 |           8 |           66 |         7,451 |                    |


Raw samples + Prometheus snapshots: [`results/oracle/orders-snapshot/2026-08-12T17-33-13Z.json`](results/oracle/orders-snapshot/2026-08-12T17-33-13Z.json)

### Snapshot analysis — first snapshot-path numbers for oracledb_cdc (2026-08-12)

Everything above this run measures LogMiner streaming; this measured
`stream_snapshot` over a pre-seeded 30M-row / 36 GB table (bounded-dataset
mode: warmup 0, snapshot visible from t=0). Two arms, one variable: go-ora's
default `PrefetchRows=25` vs `?PREFETCH_ROWS=1000` on the connection string.

Per-minute delivered MB/s (self-report, 899 samples/arm):

| arm | shape |
|---|---|
| s0-prefetch-default | flat **7.9-8.3** for all 15 min (7.0 GB of 36 GB delivered) |
| s1-prefetch-1000 | **70.7 → 51** for ~2 min, then flat **~9.3** (14.3 GB delivered) |

CloudWatch on the DB during the windows (5-min averages):

- s0: physical reads 2-3 MB/s, DB CPU 11-15% — the database was nearly idle
  while delivering 8 MB/s. **The default snapshot is network-round-trip-bound:**
  25 rows/trip at ~7,000 rows/s ≈ 280 trips/s (~3.6 ms/trip). This confirms the
  prefetch playbook's arithmetic on the snapshot SELECT path.
- s1 cold phase: physical reads **109-126 MB/s and 2,200-2,500 read IOPS to
  deliver 9.3 MB/s of rows (~12× read amplification)**, DB CPU ~20%. The fast
  first ~2 minutes (≈7 GB at 66-70 MB/s) is almost exactly the range arm s0 had
  just scanned — i.e. buffer-cache-warm blocks read at the new, higher
  trip-size limit; the collapse is where the cache runs out.

Mechanism (code-confirmed pagination, hypothesis on the I/O): the snapshot
paginates by keyset — `WHERE pk > :last ORDER BY pk FETCH FIRST n ROWS ONLY`
(`replication/snapshot.go::querySnapshotTable`). That is the right pagination
shape, but fetching full rows in PK order over a table whose physical row
order does not match PK order degenerates into scattered single-block reads
via the index — this table was seeded by 16 concurrent workers, so ids
interleave across blocks. A sequentially-loaded customer table would cluster
better; a fragmented or heavily-updated one would not.

**Takeaways:**

1. `?PREFETCH_ROWS=1000` is free and removes the round-trip cap (8.6× while
   pages are cache-warm). Recommend it for any snapshot.
2. On cold data the snapshot becomes random-read-bound at the storage layer;
   prefetch alone barely helps (9.3 vs 8 MB/s). A 36 GB table extrapolates to
   ~75 min (default) / ~55 min (prefetch) on this rig.
3. Connector-side candidate fix: snapshot in physical order (ROWID ranges /
   parallel chunks, the DBMS_PARALLEL_EXECUTE pattern) instead of PK order —
   turns scattered single-block reads into multiblock scans and parallelises
   naturally.

Caveats: single instance class (db.r5.2xlarge, 24K IOPS gp3); arm order means
s1 inherited a partially warm cache (its fast phase measures the warm-cache
regime, not steady-state); PK-vs-physical-order scatter is worst-case-ish here
because of the 16-worker interleaved seed.
