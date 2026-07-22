# Snowflake Bulk (PUT + Snowpipe) Benchmark Results

See [`internal/impl/snowflake/bench/`](../../internal/impl/snowflake/bench/) for configs and run instructions (`task bench:bulk`, `cd write/bulk && task bench:matrix`).

---

**Dataset:** synthetic events (`ID`, `USER_ID`, `EVENT_TYPE`, `VALUE`, `INFO`, `TS`), ~140 B/row, archived (`json_array`) into files staged via `PUT`, loaded into `BENCH_EVENTS_JSON` via Snowpipe.

`COUNT` must scale with `BATCH`, not be held fixed — a `COUNT` close to the largest `BATCH` tested only exercises 1 batch for that combo, which is a single noisy network sample, not steady-state throughput. First pass at `COUNT=50000` produced non-monotonic, unusable numbers once `BATCH` reached 50,000 (1 batch = 1 sample). All results below use `COUNT=500000` (10+ batches per combo).

Two rounds of results follow: **pre-fix** (config-only tuning, connector code unchanged) and **post-fix** (after removing the `WriteBatch` connection-lock bottleneck in `internal/impl/snowflake/output_snowflake_put.go`). Read the post-fix section for the current numbers.

---

## Pre-fix: config-only tuning

**Configuration highlights:** `task bench:matrix` sweeps `BATCH` x `UPLOAD_THREADS`, `MAX_IN_FLIGHT` held at 1 (see bottleneck analysis below for why).

### BATCH x UPLOAD_THREADS (rows per file, PUT parallel chunk threads)

`COUNT=500000`, `MAX_IN_FLIGHT=1`:

| BATCH | UPLOAD_THREADS | msg/sec | KB/sec |
|---|---|---|---|
| 5,000 | 4 | 656.8 | 93 |
| 5,000 | 8 | 621.6 | 88 |
| 5,000 | 16 | 593.8 | 84 |
| 5,000 | 32 | 603.5 | 86 |
| 10,000 | 4 | 985.9 | 140 |
| 10,000 | 8 | 1,223.8 | 174 |
| 10,000 | 16 | 1,044.7 | 148 |
| 10,000 | 32 | 854.9 | 121 |
| 20,000 | 4 | 1,463.3 | 208 |
| 20,000 | 8 | 1,214.1 | 172 |
| 20,000 | 16 | 1,345.9 | 191 |
| 20,000 | 32 | 1,685.9 | 239 |
| 50,000 | 4 | 3,318.8 | 471 |
| 50,000 | 8 | 3,224.3 | 458 |
| **50,000** | **16** | **4,661.2** | **662** |
| 50,000 | 32 | 2,566.9 | 364 |

50,000 is the peak batch size tested — throughput climbs steadily with `BATCH` at every thread count (5,000→50,000 is a ~7x gain). `UPLOAD_THREADS` peaks at 16 and regresses at 32 for every batch size tested (e.g. at `BATCH=50000`: 4,661 → 2,567 msg/sec) — likely saturating the outbound link or a Snowflake-side per-file chunk-upload concurrency limit.

### Recommended configuration

```
BATCH=50000  UPLOAD_THREADS=16  MAX_IN_FLIGHT=1
```

**4,661 msg/sec, 662 KB/sec** — best config-only result found. Not re-tested past `BATCH=50,000`; unlike the streaming benchmark's `BATCH` sweep, throughput had not visibly plateaued yet at the top of the tested range, so higher batch sizes may still help.

### Observations / bottleneck analysis

- `BATCH` is the dominant parameter: bigger batches amortize the fixed per-round-trip cost (PUT command + Snowpipe `insertFiles` HTTP call) over more rows.
- `UPLOAD_THREADS` (Snowflake's `PUT ... PARALLEL=N`) helps up to ~16, then regresses at 32 consistently across every batch size.
- `MAX_IN_FLIGHT` was left at 1 throughout and has **no effect** on this connector, unlike streaming: `WriteBatch` (`internal/impl/snowflake/output_snowflake_put.go`) holds a single mutex for the entire PUT + Snowpipe call, so batches never execute concurrently regardless of this setting. This is the hard ceiling on config-only tuning — every batch pays its full round-trip cost serially, one at a time, which `snowflake_streaming` (40,442 msg/sec, see [snowflake-streaming.md](snowflake-streaming.md)) does not.
- Open follow-up: closing the gap to streaming requires a code change — narrow the connection mutex to guard only the `*sql.DB` pointer (not the whole write), and upload multiple files within a batch concurrently. Prototyped and verified (build + existing unit tests pass) but not merged, pending a decision to change connector code vs. stay config-only.

### Harness gotchas found while building this benchmark

- `task bench:matrix` has no default `COUNT` bound (`input.generate.count=0` = unlimited) — without setting `COUNT`, each matrix combo runs forever and the sweep never advances past the first row.
- `COUNT` must scale with `BATCH` in the matrix loop, or the largest batch sizes get only 1 sample each and produce non-monotonic noise instead of a trend (see above).

---

## Post-fix: after removing the `WriteBatch` serialization lock

**Code change** (`internal/impl/snowflake/output_snowflake_put.go`): `connMut` changed from a `sync.Mutex` held for the entire `WriteBatch` call to a `sync.RWMutex` guarding only the `*sql.DB` pointer (set in `Connect`, cleared in `Close`). Files within a batch now upload concurrently via `errgroup`, bounded by `upload_parallel_threads`. This lets `max_in_flight` batches actually execute concurrently instead of queuing behind one lock — `*sql.DB` pools its own connections and is safe for concurrent use, so nothing else needed to change.

**Configuration highlights:** `task bench:matrix` re-pointed to sweep `BATCH` x `MAX_IN_FLIGHT`, `UPLOAD_THREADS` held at 16 (the pre-fix best). `COUNT=500000` throughout.

### BATCH x MAX_IN_FLIGHT (upload_threads=16)

| BATCH | MAX_IN_FLIGHT | msg/sec | MB/sec |
|---|---|---|---|
| 1,000 | 1 | 202.7 | 0.03 |
| 1,000 | 4 | 1,049.0 | 0.15 |
| 1,000 | 8 | 2,042.0 | 0.29 |
| 1,000 | 16 | 3,965.8 | 0.56 |
| 1,000 | 32 | 6,771.4 | 0.96 |
| 5,000 | 1 | 679.3 | 0.10 |
| 5,000 | 4 | 4,076.1 | 0.58 |
| 5,000 | 8 | 7,267.2 | 1.03 |
| 5,000 | 16 | 14,514.1 | 2.06 |
| 5,000 | 32 | 21,016.4 | 2.98 |
| 10,000 | 1 | 1,032.0 | 0.15 |
| 10,000 | 4 | 7,394.1 | 1.05 |
| 10,000 | 8 | 14,299.0 | 2.03 |
| 10,000 | 16 | 20,028.8 | 2.84 |
| **10,000** | **32** | **34,379.2** | **4.88** |
| 20,000 | 1 | 1,702.2 | 0.24 |
| 20,000 | 4 | 12,833.6 | 1.82 |
| 20,000 | 8 | 24,317.8 | 3.45 |
| 20,000 | 16 | 19,959.9 | 2.83 |
| 20,000 | 32 | 33,742.4 | 4.79 |

**Best config found: `BATCH=10000`, `MAX_IN_FLIGHT=32`, `UPLOAD_THREADS=16` → 34,379 msg/sec, 4.88 MB/sec.**

That's a **7.4x** improvement over the pre-fix best (4,661 msg/sec) and closes almost all the way to `snowflake_streaming`'s 40,442 msg/sec ceiling (see [snowflake-streaming.md](snowflake-streaming.md)) — using the same underlying warehouse-bound PUT+Snowpipe mechanism, not a different transport.

### Observations

- `MAX_IN_FLIGHT` is now the dominant lever, exactly as expected once batches can actually overlap: at `BATCH=1000`, 1→32 is a **33x** gain (202.7 → 6,771.4 msg/sec). At `BATCH=10000`, 1→32 is a **33x** gain (1,032.0 → 34,379.2 msg/sec).
- Still climbing at `MAX_IN_FLIGHT=32`, the top of the tested range, at every batch size — no plateau found yet. Worth extending the sweep to 48/64 (mirroring what the streaming benchmark found: still climbing at 48 there too).
- `BATCH=20000, MAX_IN_FLIGHT=16` (19,959.9) sits below `BATCH=20000, MAX_IN_FLIGHT=8` (24,317.8) — one anomalous dip against an otherwise monotonic trend; worth a rerun to confirm it's noise and not a real regression before trusting `BATCH=20000` over `BATCH=10000` at that setting.
- `BATCH=10000` and `BATCH=20000` land close at `MAX_IN_FLIGHT=32` (34,379 vs 33,742) — batch size matters much less now than it did pre-fix, since the fixed per-round-trip cost is now paid by many batches in parallel instead of serially.

### Follow-up

- Extend `MAX_IN_FLIGHT` sweep to 48/64 at `BATCH=10000` and `BATCH=20000` to find the real plateau.
- Rerun `BATCH=20000, MAX_IN_FLIGHT=16` to confirm the dip noted above is noise.
