# Snowflake Snowpipe Streaming Benchmark Results

See [`internal/impl/snowflake/bench/`](../../internal/impl/snowflake/bench/) for configs and run instructions (`task bench:streaming`, `task bench:streaming:matrix`).

---

**Dataset:** synthetic events (`ID`, `USER_ID`, `EVENT_TYPE`, `VALUE`, `INFO`, `TS`), ~140 B/row, into typed table `BENCH_EVENTS`.

**Configuration highlights:** one dimension swept at a time via `task bench:streaming:matrix` (`BATCH`, `PARALLELISM`, `MAX_IN_FLIGHT`, `CHUNK_SIZE`), others held at default (`BATCH=1000 PARALLELISM=1 MAX_IN_FLIGHT=4 CHUNK_SIZE=50000`). `COUNT` bounds each run so it terminates and prints one final summary instead of requiring Ctrl-C.

Small `COUNT` is noisy — at `COUNT=10000` channel-open/connection overhead dominates the measurement window and results don't trend cleanly. Meaningful signal only showed up at `COUNT=200000` and above.

### BATCH (message count per Snowpipe Streaming batch)

`COUNT=200000`, others at default:

| BATCH | msg/sec |
|---|---|
| 1,000 | 3,354 |
| 2,000 | 6,041 |
| 4,000 | 8,837 |
| 8,000 | 16,112 |
| 16,000 | 15,430 |
| 32,000 | 26,597 |
| 64,000 | 27,763 |

`COUNT=1,000,000`, confirming the top end:

| BATCH | msg/sec | KB/sec |
|---|---|---|
| 32,000 | 42,864 | 6,093 |
| **64,000** | **43,281** | **6,153** |
| 128,000 | 41,472 | 5,896 |
| 256,000 | 40,914 | 5,816 |

64,000 is the peak — throughput climbs steadily up to it, then declines past it.

### PARALLELISM (parquet build parallelism)

`COUNT=1,000,000`, `BATCH=1000` (default), others at default:

| PARALLELISM | msg/sec |
|---|---|
| 1 | 3,839 |
| 2 | 3,855 |
| 3 | 3,769 |
| 4 | 3,704 |
| 5 | 3,857 |
| 6 | 3,750 |
| 7 | 3,601 |
| 8 | 3,785 |

Flat within ~7% (3,600-3,860 msg/sec) across all values — noise, not a trend. Not worth tuning past 1.

### MAX_IN_FLIGHT (concurrent Snowpipe Streaming channels)

`BATCH=1000` (default), others at default:

| MAX_IN_FLIGHT | msg/sec | COUNT |
|---|---|---|
| 1 | 851 | 200,000 |
| 2 | 1,863 | 200,000 |
| 4 | 3,377 | 200,000 |
| 8 | 5,978 | 200,000 |
| 16 | 12,642 | 1,000,000 |
| 24 | 12,742 | 1,000,000 |
| 32 | 12,382 | 1,000,000 |
| 48 | 16,951 | 1,000,000 |

Roughly linear scaling at `BATCH=1000` (small batches build fast, so more concurrent channels keep saturating). Still climbing at 48 — no plateau found. Not re-tested at `BATCH=64,000`, where each channel build takes longer; 16 was used as a conservative middle value for the recommended config below.

### CHUNK_SIZE (rows per parquet chunk)

`COUNT=200,000`, `BATCH=1000` (default), others at default — no clear signal:

| CHUNK_SIZE | msg/sec |
|---|---|
| 6,250 | 3,707 |
| 12,500 | 3,409 |
| 25,000 | 2,428 |
| 50,000 | 2,266 |
| 100,000 | 2,798 |
| 200,000 | 3,098 |
| 400,000 | 3,106 |

Left at default (50,000). Low priority — noisy, no strong trend either direction.

### Recommended configuration

```
BATCH=64000  MAX_IN_FLIGHT=16  PARALLELISM=1  CHUNK_SIZE=50000
```

Confirmed at scale:

```
task bench:streaming COUNT=5000000 BATCH=64000 MAX_IN_FLIGHT=16 PARALLELISM=1 CHUNK_SIZE=50000
```

```
INFO total stats: 40441.766784 msg/sec, 5.8 MB/sec
```

**40,442 msg/sec, 5.82 MB/sec** sustained over 5,000,000 messages.

### Observations / bottleneck analysis

- `BATCH` and `MAX_IN_FLIGHT` are the two parameters that matter; `PARALLELISM` and `CHUNK_SIZE` are noise within the tested ranges.
- `MAX_IN_FLIGHT` hadn't plateaued at 48 (tested only at `BATCH=1000`) — untested whether higher values (64-128) help at the winning `BATCH=64,000`, since larger batches take longer to build per channel and may need fewer concurrent channels to saturate.
- Open follow-up: re-run `MAX_IN_FLIGHT` sweep (16/24/32/48/64) at `BATCH=64000` to see if the recommended config leaves throughput on the table.

### Harness gotchas found while building this benchmark

- `ACCOUNTADMIN` (a tempting default role) isn't granted to every user — no safe default role exists; must be set explicitly per account.
- `PUBLIC` (a tempting default schema) may not exist on the account — this account uses `RAW`/`PROCESSED`/`CONFIG` instead.
- `logger.level: WARN` in `benchmark_config.yaml` silently swallowed the `benchmark` processor's throughput logs (they're `INFO`) — including the final "total" summary, not just the rolling stats.
- `benchmark.interval: 1s` produces a rolling stats line every second; set to `0s` to log only the final "total" summary on shutdown, per the processor's own docs.
- Go logs large `bytes/sec` values in scientific notation (e.g. `8.981e+06`) — a naive `grep -oP '[0-9.]+'` truncates at the `e`, silently corrupting derived KB/sec figures for high-throughput runs. Widen the regex to `[0-9.eE+-]+` when parsing structured log fields for scripted result tables.
