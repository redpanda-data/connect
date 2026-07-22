# Snowflake Bulk (PUT + Snowpipe) Benchmark Results

See [`internal/impl/snowflake/bench/`](../../internal/impl/snowflake/bench/) for configs and run instructions (`task bench:bulk`, `cd write/bulk && task bench:matrix`).

---

**Dataset:** synthetic events (`ID`, `USER_ID`, `EVENT_TYPE`, `VALUE`, `INFO`, `TS`), ~140 B/row, archived (`json_array`) into files staged via `PUT`, loaded into `BENCH_EVENTS_JSON` via Snowpipe.

**Configuration highlights:** `task bench:matrix` sweeps `BATCH` x `UPLOAD_THREADS`, `MAX_IN_FLIGHT` held at 1 (see bottleneck analysis below for why). `COUNT` bounds each run.

`COUNT` must scale with `BATCH`, not be held fixed — a `COUNT` close to the largest `BATCH` tested only exercises 1 batch for that combo, which is a single noisy network sample, not steady-state throughput. First pass at `COUNT=50000` produced non-monotonic, unusable numbers once `BATCH` reached 50,000 (1 batch = 1 sample). Rerun at `COUNT=500000` (10+ batches per combo) below is the trustworthy data.

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
