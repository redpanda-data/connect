# Range-Chunked Initial Snapshots for CDC Inputs

## Purpose

A CDC input usually begins with an initial snapshot of every captured table, then streams changes
from the position where the snapshot was taken. Reading each table with one reader makes the
snapshot as slow as the largest table. Range chunking splits a large table into primary key ranges
that several readers read in parallel. All the ranges still show the database at one point in
time, so the hand-over to streaming stays correct.

This document states the required behaviour independently of any one database, so it can be
implemented in other CDC inputs. The Oracle implementation (`internal/impl/oracledb`, described in
`internal/impl/oracledb/SNAPSHOT_CHUNKING.md`) is the reference. The edge cases below were found
and measured while building it.

The key words MUST, SHOULD and MAY are used as in RFC 2119.

### How this differs from `incrementalsnapshot`

`internal/replication/incrementalsnapshot` backfills tables in key-ordered chunks while the stream
is already running. It uses watermarks to drop rows the stream has already delivered. This document
covers the other approach: a point-in-time snapshot taken before streaming starts. Use this one
when the database can read every chunk at the same point in time. Use `incrementalsnapshot` when
it can't, or when the snapshot must not delay streaming.

## Terms

- **Snapshot point (P):** the position in the change stream (an SCN, LSN, binlog position or slot
  LSN) where the snapshot is taken.
- **Chunk:** a `(min, max]` primary key range of one table, read by one task. A nil bound is
  unbounded.
- **Worker:** one of the `max_parallel_snapshot_tables` readers. Each worker holds one database
  connection while it runs a task.
- **Task:** a unit of work for one worker: one chunk, one whole-table scan, or one filtered-table
  read.

## Requirements

### Consistency

- **R1.** Every chunk of every table MUST read the database as of the same snapshot point P, or at a
  point no earlier than P, provided streaming makes up for the difference (R4).
- **R2.** Chunks of one table MUST cover its whole key range exactly once. Adjacent chunks share a
  bound (one's exclusive minimum is the previous one's inclusive maximum), the first chunk has no
  minimum, and the last has no maximum.
- **R3.** Within a chunk, rows MUST be read in pages ordered by the full primary key, and each page
  MUST continue strictly after the last key of the previous page. Sorting and comparing keys MUST
  use the same order (R11).
- **R4.** Streaming MUST start from a position that delivers every change committed after the state
  the snapshot read. A change MUST NOT fall between the two. Duplicates at the boundary are allowed
  (at-least-once delivery). Loss isn't.
- **R5.** The stream position MUST NOT be checkpointed until every snapshot row has been delivered
  and acknowledged downstream. A crash before then MUST re-run the snapshot.

How each database meets R1 and R4:

| Database | Shared point in time for all workers | Where streaming starts | Open transactions at P |
|---|---|---|---|
| Oracle | `AS OF SCN P` flashback queries on any connection | LogMiner from SCN P | **Not covered**: LogMiner mines raw redo, so changes an open transaction made before P are lost (see E19) |
| PostgreSQL | Export a snapshot when creating the replication slot, then `SET TRANSACTION SNAPSHOT` in each worker's `REPEATABLE READ` transaction | The slot, from its consistent point | Covered: logical decoding emits each transaction whole when it commits |
| MySQL | `FLUSH TABLES … WITH READ LOCK`, then `START TRANSACTION WITH CONSISTENT SNAPSHOT` in every worker before the lock is released | Binlog position read under the lock | Covered: a transaction reaches the binlog as a whole when it commits |
| SQL Server | `SNAPSHOT` isolation per transaction. Each one starts when it first reads, so workers don't share a point | `fn_cdc_get_max_lsn()` read before any worker starts | Covered: change rows carry their transaction's commit LSN |

If the database can't share one point in time across connections, as with SQL Server, each worker
MUST start its transaction after P is captured. R4 then holds because streaming from P replays
every change after P, and rows a worker already saw arrive again as duplicates.

### Planning

- **R6.** A table MUST be chunked only if it has a primary key that the database guarantees unique
  and non-NULL, such as Oracle's `ENABLED VALIDATED`, or the equivalent. Otherwise it MUST be read
  with one whole-table scan. NULL keys match no range, and duplicate keys can be skipped at a page
  boundary.
- **R7.** A table MUST NOT be chunked when its custom snapshot query (filter) defines which rows it
  reads. Such a table is read by one task.
- **R8.** A table MUST NOT be split into chunks smaller than a minimum row count (the reference uses
  10,000). The number of chunks MUST be at most `rows / minimum`, and a table that allows fewer than
  2 chunks MUST get one whole-table scan.
- **R9.** The row count used for R8 SHOULD come from optimizer statistics, which are cheap to read.
  When statistics are missing or show the table as too small to chunk, the implementation SHOULD
  count the rows, stopping at the most any plan could need: `planned ranges × minimum`. The count
  MUST NOT remove the cap in R8.
- **R10.** Range planning SHOULD interpolate between the key's MIN and MAX for single-column keys
  with an evenly spaced type (integer, numeric, date, timestamp). It SHOULD fall back to sampling
  keys when:
  - the key can't be interpolated, or
  - fewer than half the interpolated ranges hold a row.

  A sample's size MUST come from the best row count available (R9), not from stale statistics.
  Sampling that needs a row count MUST be skipped when there's none, and the table read with one
  scan.

### Ordering and comparison

- **R11.** Every connection used for planning or reading chunks MUST sort and compare keys in the
  same order. A session-level linguistic or case-insensitive collation MUST be overridden to binary
  (for example `NLS_SORT`/`NLS_COMP` on Oracle, or `COLLATE` or the session collation elsewhere).
  Binary is the order a primary key's uniqueness is defined in.
- **R12.** Key values read for bounds and page continuation MUST bind back with the same value and
  type the database compares against. Watch for drivers that attach time zones to zone-less date
  types, lose numeric precision (float64, or integers above 2^53), or truncate fractional seconds.

### Scheduling and connections

- **R13.** Tasks from every table MUST share one pool of `max_parallel_snapshot_tables` workers.
  Single-task tables SHOULD be started before chunk tasks, so their one serial read overlaps the
  chunk work instead of running alone at the end.
- **R14.** When the database refuses a connection for capacity reasons, workers MUST wait and retry
  with a configurable backoff instead of failing:
  - **Error codes:** these include Oracle ORA-00018, ORA-00020 and ORA-12516–12520, PostgreSQL
    `too_many_connections`, and MySQL `ER_CON_COUNT_ERROR`.
  - **When to fail:** a worker MAY fail only when its backoff runs out while no other worker holds
    a connection. While one does, the snapshot is still making progress.
- **R15.** A connection MUST count as held from the moment the pool returns it, before the caller
  uses it. A worker whose backoff has run out SHOULD try once more before failing, in case another
  worker has just released one.
- **R16.** Every per-connection setup, such as switching container, session settings or collation
  (R11), MUST be applied to every connection a task uses, not once to the pool. Any cleanup that runs
  after cancellation MUST have its own timeout, so a hung database can't block shutdown.
- **R17.** Startup checks and reading the snapshot point SHOULD go through the same connection path,
  so they also wait with backoff.

### Compatibility

- **R18.** Turning chunking on MUST NOT add a privilege requirement that fails existing
  configurations. If a chunked read needs an extra privilege that the user lacks, such as Oracle's
  `FLASHBACK`, the table MUST fall back to the reading method that worked before. A warning naming
  the missing grant MUST be logged. Match every error code the database versions report: Oracle
  23ai reports ORA-41900 where older versions report ORA-01031.
- **R19.** Fallbacks (R6–R10, R18) MUST be logged with the table name and the reason, at Info level,
  or Warning for R18, so users can see why a table wasn't parallelised.

### Configuration and documentation

- **R20.** Reuse the shared `MaxParallelSnapshotTablesField(extra)` helper in `internal/replication`, passing
  connector-specific text as `extra`.
- **R21.** The field docs MUST say:
  - when a table isn't split, and the minimum row count;
  - that each parallel reader holds a connection, and how capacity errors are handled;
  - any extra privilege or retention requirement, such as Oracle's undo retention for flashback
    reads.
- **R22.** Durations such as backoffs and timeouts MUST be configurable, unless they only bound an
  internal race or a shutdown path. In that case, a comment MUST say why.

## Edge cases

Each case lists the risk, the requirement that covers it, and what the reference implementation
measured.

| # | Case | Risk | Requirement | Reference measurement |
|---|---|---|---|---|
| E1 | Table with a custom snapshot filter | Range planning ignores the filter | R7 | Never chunked |
| E2 | No primary key | No order to page by | R6 | Full scan |
| E3 | Primary key not enforced (NULLs, duplicates) | NULL keys dropped; duplicates skipped at page boundaries | R6 | Integration test with NULL and duplicate keys |
| E4 | Composite key | No tuple comparison in SQL | R3 | `(a > x) OR (a = x AND b > y)` |
| E5 | String or UUID key | Can't be interpolated | R10 | Sampled. A 4M-row UUID table: 17s → 7s at 12 workers |
| E6 | Many small tables | Each chunk pays connection setup, so many tiny chunks are slow | R8 | Before: 200 × 1,000-row tables took 24s at 8 workers, against 1s with one reader. After: 2s |
| E7 | Missing statistics | Can't size the plan | R9 | Capped count |
| E8 | Stale statistics showing a small table | Large table not chunked, or a count with no cap gives tiny chunks | R9 | Integration test with statistics forced to 1,000 rows on 30,000 |
| E9 | Stale statistics sizing a sample | Sample reads far too many rows | R10 | 1M rows with statistics at 2,000: sample went from 128,319 rows to 257 |
| E10 | No statistics and a key that must be sampled | Count wasted, up to 1.28M rows at 8 workers | R10 | Count skipped; full scan |
| E11 | Outlier key, for example one far sentinel | Equal-width ranges put every row in one chunk | R10 | Sampled after the empty-range check. A 4M-row table: 15s → 5s |
| E12 | Unevenly spread keys (dense band, not empty elsewhere) | Passes the empty-range check, so one chunk holds most rows | Open | 90% of rows in one chunk; barely faster than one reader |
| E13 | Small table with an outlier (under about 5 × minimum rows) | Too few ranges for the empty-range check to work | Open, accepted | Still correct; effectively one reader |
| E14 | Linguistic or case-insensitive session collation | Rows skipped or read twice | R11 | 30,000 rows under `NLS_SORT=GERMAN`: 15,000 lost, 8,258 duplicated before the fix |
| E15 | Driver changes date or time key values | Bounds shift by a time zone offset | R12 | Checked on a server at -05:00: not affected with go-ora v2.9.0 |
| E16 | Database at capacity | Snapshot fails, or a worker gives up just as another is acquiring or releasing | R14, R15 | 128 workers hit ORA-12516, waited, and finished correctly |
| E17 | Connection closed during login with no error code | Down and full look the same | R14 | Retried. The final error says the database may be full or down |
| E18 | Missing privilege for consistent reads | Existing configs fail after upgrading | R18 | Fallback to full scan, with a warning naming the grant |
| E19 | Transaction open at P | Its changes before P are in neither the snapshot nor the stream | R4 | Lost on Oracle. Not affected on PostgreSQL, MySQL or SQL Server |
| E20 | Snapshot retention runs out (undo, MVCC horizon) | Reads fail partway through ("snapshot too old") | R21 | Documented; no fallback |
| E21 | Transient error just after P is captured | Spurious failures | Retry with a short, fixed bound (R22) | Oracle ORA-01466: 5 retries, 100 ms apart |
| E22 | Hung database during connection cleanup | Shutdown blocks | R16 | 10s bound on the container switch back |

### E19 in detail

Suppose a transaction T writes a row before P and commits after P.
- **Snapshot:** a read at P only sees committed data, so it doesn't see T's write.
- **Streaming:** whether it sees the write depends on what the change source exposes.

| Change source | What it exposes | Result |
|---|---|---|
| Logical decoding, binlog, CDC change tables | Only committed transactions, each whole, at its commit position | T is delivered in full after P. No loss. |
| Raw redo or WAL mined from P (LogMiner without `COMMITTED_DATA_ONLY`) | Records in write order, starting at P | T's writes before P are never read. **Loss.** |

An implementation on a raw source MUST do one of these:
- **Mine from earlier:** start streaming from the oldest open transaction's start position at P,
  and skip transactions that committed at or before P.
- **Wait:** wait until no transaction older than P is open before taking the snapshot.

Streaming checkpoints usually already stay below the oldest open transaction. The snapshot
hand-over must too.

## Verification

### Correctness

A chunked snapshot MUST be checked against the database at P, not just by counting rows:

- **Row fingerprint:** render each row as its column values joined by a separator, in a canonical
  form (fixed date and number formats, NULL as a marker, LOBs as length plus a hash of each part),
  and hash it.
- **Table fingerprint:** the row count, `MIN(key)`, `MAX(key)`, `COUNT(DISTINCT key)`, and the sum of
  a fixed-width prefix of every row hash. The sum doesn't depend on row order, but changes if any
  value in any row differs.
- **Compare both sides:** compute the fingerprint from the output and, in the database, as of P.
  Report duplicate keys and rows with unexpected columns separately.

### Tests to include

1. Every key type you support (integer, fractional numeric, date, timestamp with fractional
   seconds, string, composite) returns every key exactly once. Use a small page size, so page
   boundaries fall inside chunks.
2. Inserts, updates, deletes and key updates made after P are excluded from the snapshot and
   delivered by the stream.
3. Each fallback in R6–R10 and R18 is taken, and logged.
4. A session with a linguistic collation, for example set by a logon trigger, returns every key
   exactly once (R11).
5. A transaction open across P is delivered in full: once by the stream, or by both with duplicates
   allowed, but never partly lost (R4, E19).
6. Capacity handling: giving up only when no connection is held, and recovering when one is
   released (R14, R15).
7. A failing publisher, and a cancelled context during the snapshot, stop every worker and release
   every connection.

### Load tests

Run each against a container with a fixed data set, resetting the stored position before each run.

| Test | Shape | What it shows |
|---|---|---|
| Worker scaling | About 20M rows over 3 tables, including LOB columns; `max_parallel_snapshot_tables` of 1, 3, 12, 50 and 128 | Speed-up, the best worker count, and capacity handling at high N. All runs must match the fingerprint. |
| Table sizes | One table each of 100, 10K, 100K, 1M and 5M rows; 1 and 12 workers, 3 runs each | Small tables aren't split (R8); speed-up grows with size |
| Many small tables | 50 and 200 tables of 1,000 rows; 8 and 12 workers | Per-table planning overhead stays close to one reader |
| Skewed keys | 4M rows each: an outlier key, a dense band, and UUID keys | The empty-range fallback and sampling work; open cases are measured |

Reference results, from the Oracle connector on a laptop:

| Test | One reader | 12 workers |
|---|---|---|
| 22.78M rows over 3 tables | 171s | 45s |
| 5M-row table | 14.7s | 4.7s |
| 1M-row table | 2.4s | 0.9s |
| 100K-row table | 230 ms | 154 ms |
| 100-row table (not split) | 6 ms | 23 ms (planning queries) |

Harness pitfalls:
- **Output pipeline:** make sure it isn't the bottleneck. Per-row mapping or plain per-message
  output limited the reference to about 170K rows a second, which hid any parallel speed-up at 5M
  rows. Batch rows into one message (for example with the `archive` processor), or use `drop`.
- **Table selection:** anchor table-selection patterns if they're regular expressions.
- **Change-source prerequisites:** check them before timing anything, for example supplemental
  logging or replica identity. A missing one can make the input retry forever, which looks like a
  slow run.
- **Change-source polling:** the stream may only pick up changes on its next poll or log switch.
  Wait for a control change written after P before deciding a change was lost.
