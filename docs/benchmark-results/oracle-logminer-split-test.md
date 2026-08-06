# Oracle CDC performance: how LogMiner and `oracledb_cdc` behave

Findings from three AWS benchmark runs, 2026-08-05 and 2026-08-06, us-east-2.

**Summary**

- Throughput is bounded **per reader**, not per database. One reader sustains roughly
  **7-12 MB/s** of change data live, or **~19 MB/s** when draining a backlog.
- Adding readers scales **sub-linearly** and plateaus near **31 MB/s** per database.
- **Cores are not the constraint.** Connect never exceeded **1.0 of 4 cores** in any
  configuration tested, including at its ceiling.
- On the database, the **write workload dominates** CPU (~32-34% of 8 vCPU at 35 MB/s of
  inserts). Each reader adds a fixed scan cost; returning matched rows is cheap. **Tables are
  nearly free to add to a reader; readers are not.**
- Recommended shape: **a few readers per database, each covering many tables.** Never one
  reader per table.
- **Unexplained:** why throughput plateaus at 5 readers when neither side is CPU-exhausted.

---

## 1. Mechanism

Oracle writes every change for a database into **one ordered redo stream**. There is one per
database and it cannot be split. LogMiner decodes it; `oracledb_cdc` reads the decoded rows.

Per reader the loop is: start a LogMiner session over the relevant log files, `SELECT` from
`V$LOGMNR_CONTENTS` for one SCN range, pull the matching rows, **parse each one inline on the
same goroutine**, advance the SCN, repeat (`internal/impl/oracledb/logminer/logminer.go`).
Four consequences explain every measurement below:

- **Table selection is a server-side predicate** on `(SEG_OWNER, TABLE_NAME)`. Non-matching
  rows never cross the wire and are never parsed — so a reader covering 1 of 5 tables genuinely
  pays for a fifth of the rows.
- **Reading the view is what causes the scan.** `V$LOGMNR_CONTENTS` is generated as it is read,
  not stored, so decode cost lands on Oracle's CPU while Connect waits.
- **One session is strictly serial.** Window N+1 cannot start until N is parsed, because
  transactions must be consumed in SCN order. This is the per-reader ceiling.
- **Transaction control is not filtered.** START/COMMIT/ROLLBACK return for *every* transaction
  on the database, since transaction boundaries are needed regardless of table. A reader
  matching no tables still receives this traffic.

LogMiner materialises **no copy** of the data, and with `DICT_FROM_ONLINE_CATALOG` stages no
dictionary snapshot. But enabling CDC does add storage: supplemental logging inflates redo
volume, and that redo must be retained to cover an outage **plus catch-up time**. In-flight
transactions are buffered by **Connect**, in memory by default, until COMMIT arrives.

## 2. Test rig

Identical across all three runs:

| Component | Specification |
|---|---|
| Source database | RDS Oracle **SE2 19c**, non-CDB, `db.r5.2xlarge` (8 vCPU / 64 GiB, x86 — no Graviton option), 800 GB gp3, ARCHIVELOG 24 h |
| Connect | `c8g.4xlarge`, pinned to **4 vCPU** via `taskset`, GOMAXPROCS 8 |
| Redpanda | 3 × `im4gn.2xlarge` |
| Workload | 5 tables × 7.0 MB/s = **35.0 MB/s** (30,580 rows/s at 1200 B), delivered on target and logged per table. **Inserts only** |
| Logging | Database-level minimal + PK supplemental logging; per-table `SUPPLEMENTAL LOG DATA (ALL) COLUMNS` |
| Windows | 2 min warmup + 15 min measured (30 min on the third run); tables truncated between arms |
| Method | Arms of a single sweep — same instance, same live load, one variable per comparison. Database CPU from CloudWatch `AWS/RDS CPUUtilization`, 1-minute period, averaged over each arm's window |

### Methodology note: earlier Oracle numbers were invalid

Earlier benchmarks reported ~13 MB/s *flat across 1-8 vCPU* and this was read as a LogMiner
ceiling. It was not. The load generator gated blocking inserts on a 100 ms `time.Ticker`; Go
discards missed ticks, so a worker whose insert took ~1.5 s achieved ~0.67 inserts/sec instead
of 10, capping delivered load near 12 MB/s regardless of target. Connect was keeping pace with a
load that was identical at every core count.

**Generic lesson: a ticker gating a blocking operation silently degrades to `1/latency`
throughput and reports no error.** A load generator without delivered-rate instrumentation
cannot be trusted to have delivered its target. All numbers here come from the rewritten
generator in `benchmarking/aws/seeders/cdc-rows-oracle/sql.go`, which paces continuously and
logs delivered rows/sec per table.

## 3. The ceiling belongs to the reader, not the database

Two readers, same database, same 35 MB/s load, differing only in how many of the five written
tables each captured (`oracle/orders-5table-split`):

| | Reads 1 of 5 tables | Reads all 5 |
|---|---|---|
| Captured | **6,014 of 6,116 rows/s — 98%** | **16,866 of 30,580 rows/s — 55%** |
| Mean | 7.72 MB/s | 21.33 MB/s (median 19.00) |
| Samples at 0 MB/s | **24 of 899 (2.7%)** | **0 of 900** |
| Connect CPU | 0.26 of 4 cores | 0.75 of 4 cores |

The one-table reader **ran out of work** — those zero samples are the miner reaching the
database's current SCN and sleeping on its backoff, while watching a full 35 MB/s stream. The
five-table reader never idled once and fell ~12 GB behind.

Both performed the *same* scan of the *same* stream; only matched-row volume differed. So the
limit scales with rows returned, not stream volume, and it is a property of one reader.

## 4. Sustainable rate is well below saturated rate

A single reader, by how much it was offered:

| Offered to the reader | Captured | Behaviour |
|---|---|---|
| 7.0 MB/s (1 table) | **98%** | keeps up, goes idle |
| 14.0 MB/s (2 tables) | **84.5%** | falls behind |
| 35.0 MB/s (5 tables) | **49%** (mean 19.0 MB/s) | saturated |

The 19 MB/s figure describes a reader *draining a backlog*, with a full window of rows to fetch
each cycle. A reader expected to keep pace live already misses 15% at 14 MB/s. **Plan on
7-12 MB/s per reader, not 19.**

## 5. More readers help, sub-linearly, then stop

Same 35 MB/s load split across N concurrent `oracledb_cdc` inputs under a `broker` input, each
with its own `checkpoint_cache_key` (`oracle/orders-5table-readers`):

| Readers | Captured | Mean MB/s | Connect CPU | Oracle CPU |
|---|---|---|---|---|
| 1 (all 5 tables) | 48.9% | 18.97 | 0.64 of 4 | 45.9% |
| 2 (3 + 2) | 66.9% | 25.91 | 0.87 of 4 | 50.5% |
| 5 (1 each) | **78.8%** | **30.67** | 0.99 of 4 | 72.2% |

Splitting works — **+62% from 1 to 5 readers** — because each reader is its own goroutine,
parallelising the serial fetch-and-parse cycle. But 1→2 bought +7 MB/s while 2→5 bought only
+5 more, and five readers never reached the offered 35.

**Unexplained:** at five readers *neither* side is exhausted — Oracle at 72.2% with 28 points of
headroom, Connect at 0.99 of 4 cores — yet throughput stops improving. Candidates we cannot
distinguish: contention on redo latches or buffers, diminishing overlap between readers, or a
serialised component inside Oracle's mining. **If more than ~31 MB/s from one database is
needed, this is the unknown that matters.**

## 6. Database CPU: writes dominate, tables are cheap

Under a constant 35 MB/s write load (`oracle/orders-5table-baseline`, `oracle/orders-return-cost`):

| Configuration | Oracle CPU | Confidence |
|---|---|---|
| Writes only, no reader | **32.0% / 34.4%** | High — replicated in two runs |
| + 1 reader matching no tables (scan only) | 36.1% / 46.4% | **Low — did not replicate** |
| + 1 reader returning ~12 MB/s | 37.7% | Low |
| 2 readers | 50.5% | Medium |
| 5 readers | 72.2% | Medium |

Derived figures:

- **Write path: ~33% of 8 vCPU**, before any CDC exists — the single largest consumer, and constant.
- **Marginal cost per reader: ~5-7 points**, taken from the reader sweep (larger deltas, better
  signal) rather than single-reader subtraction.
- **Cost of returning rows: <2.4 points** at ~12 MB/s — below measurement resolution.

The practical asymmetry: **a reader's scan cost is fixed and roughly independent of how many
tables it covers**, while the rows it returns cost little. Adding tables to an existing reader is
nearly free; adding a reader is not.

### Precision caveat — read before quoting the CPU figures

Per-minute database CPU has a standard deviation of ~8 points. Differences of a few points
between 30-minute averages carry ±2 points of standard error at best, and the scan-only
measurement came out at **4.12 ± 2.12 in one run and 12.0 in another** for an identical
configuration. **Treat the throughput figures as reliable and the CPU attributions as
indicative only.** Resolving effects this size needs RDS Enhanced Monitoring (1-second) or
Oracle session statistics, not 1-minute CloudWatch.

An earlier attempt to decompose scan versus row-return cost also produced a *negative* return
cost, because the comparator arm had fallen behind and therefore mined less stream in the same
window. `oracle/orders-return-cost` was built to fix that by keeping every reader arm under the
single-reader ceiling; its `a2` arm still captured only 84.5%, so that subtraction remains
partly confounded. `runner/oraclereturncost_test.go` encodes the validity gate.

## 7. Sizing guidance

| Planning number | Value |
|---|---|
| Per reader, keeping up live | **7-12 MB/s** |
| Per reader, draining a backlog | ~19 MB/s |
| Per database, practical aggregate | **~25-31 MB/s** |
| Readers per database | 3-5 — beyond that, scale the instance |
| Connect cores | **2-4 total** |

- **Group tables into a few readers; never one per table.** Tables are cheap on the database
  side, readers carry a fixed scan cost, and the throughput gain flattens fast.
- **Don't buy cores for this.** Connect never exceeded one core of four at any reader count.
- **Size retention for outage + catch-up**, not just outage. A reader near its ceiling drains
  backlog slowly — at 70% utilisation, one hour of downtime takes roughly two and a half hours
  to recover.
- **Budget storage for supplemental logging.** ALL-column logging inflates redo, retained
  longer: online redo, archive logs, backups, and any standby bandwidth.
- **Above ~31 MB/s per database the levers are a larger instance or a cheaper per-row parse
  path** — not more readers. An upstream parser rewrite (#4533) previously delivered +46% at zero
  database cost, making parse efficiency the more promising direction.
- **GOMAXPROCS will not help.** All runs used GOMAXPROCS 8 against a 4 vCPU pin while consuming
  at most 1.0 core. It cannot unserialise a single goroutine, network-parked goroutines hold no
  P, and past ~2× the CPU quota it costs more in GC and scheduler churn than it returns.

## 8. Limits

- **Insert-only workload.** With ALL-column supplemental logging, an update of two columns on a
  wide row logs the entire row — far more redo per change than an insert. These numbers
  *understate* a realistic update-heavy workload.
- **One instance class, one edition, non-CDB.** All results are db.r5.2xlarge, SE2 19c. CDB/PDB
  and RAC add mechanisms not exercised here.
- **Row width uncharacterised.** 1200 B rows throughout. An independent local harness measured
  ~50K rows/s at 167 B/row (~8 MB/s), so the ceiling is neither pure rows/sec nor pure
  bytes/sec — per-reader MB/s moves with row width.
- **The 5-reader plateau has no established cause** (§5).
- **Per-reader database CPU did not replicate** between runs (§6).
- **Harness discrepancy:** broker-derived throughput read ~13× lower than Connect-side for these
  scenarios. Connect-side figures are used throughout and are corroborated by rows × row size,
  but broker MB/s should not be quoted for Oracle until diagnosed.

## 9. Next measurement

The highest-value next step is a **pprof capture during a saturated single-reader window**, to
split waiting-on-the-wire from parsing inside the mining goroutine — that determines whether a
cheaper parse path or a larger cursor fetch size is the lever.

`http.debug_endpoints: true` is already set by the runner (`runner/main.go`), so
`/debug/pprof/{profile,trace,goroutine}` and `/debug/stack` are live during every bench, and the
runner already curls `:4195` over SSM. Note `/debug/pprof/block` is **unusable as shipped** —
nothing calls `runtime.SetBlockProfileRate` — so use `/debug/pprof/trace`, which shows
per-goroutine running/blocked/syscall states directly and needs no code change.

Explaining the 5-reader plateau is separate and larger: it needs Oracle-side instrumentation
(Enhanced Monitoring at 1 second, or `V$SESSTAT`/AWR), not Connect profiling.

## Artifacts

| Item | Path |
|---|---|
| Scenarios | `benchmarking/aws/scenarios/oracle/orders-5table-split.yaml`, `-readers.yaml`, `-baseline.yaml`, `orders-return-cost.yaml` |
| Results | `benchmarking/aws/results/oracle/<scenario>/<timestamp>.json` |
| Load generator | `benchmarking/aws/seeders/cdc-rows-oracle/sql.go` — `workload`, `reportLoadRate` |
| Arm-validity tests | `benchmarking/aws/runner/oracle{split,readers,baseline,returncost}_test.go` |

Runs: 2026-08-05 (split), 2026-08-06 (readers, baseline, return-cost). All torn down clean.
Connect-only throughout — `matrix.arms` compares Connect launch topologies and requires
`--engines=connect`.
