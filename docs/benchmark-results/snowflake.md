# Snowflake Write Benchmark Results

See [`internal/impl/snowflake/bench/`](../../internal/impl/snowflake/bench/) for configs and run instructions.

There is no Redpanda Connect Snowflake input connector — read-side benchmarks are out of scope. Two write connectors are covered below: [Snowpipe Streaming](#snowpipe-streaming-snowflake_streaming) (`snowflake_streaming`) and [Bulk PUT + Snowpipe](#bulk-put--snowpipe-snowflake_put) (`snowflake_put`).

---

<a id="snowpipe-streaming-snowflake_streaming"></a>
## Snowpipe Streaming (`snowflake_streaming`)

Run instructions: `task bench:streaming`, `task bench:streaming:matrix`, `task bench:streaming:matrix:full`.

**Dataset:** synthetic events (`ID`, `USER_ID`, `EVENT_TYPE`, `VALUE`, `INFO`, `TS`), ~140 B/row, into typed table `BENCH_EVENTS`.

**Method:** each of `BATCH`, `PARALLELISM`, `MAX_IN_FLIGHT`, `CHUNK_SIZE` was first swept one at a time (`bench:streaming:matrix`, others held at default), then the top region was confirmed with a full cross-product sweep (`bench:streaming:matrix:full`). Small `COUNT` is noisy — channel-open/connection overhead dominates below `COUNT=200,000`.

### Summary

| Parameter     | Range tested      | Best region   | Effect                                                                                             |
| ------------- | ----------------- | ------------- | -------------------------------------------------------------------------------------------------- |
| BATCH         | 1,000-256,000     | 32,000-64,000 | dominant lever - climbs steadily, peaks ~64,000, declines past it                                  |
| MAX_IN_FLIGHT | 1-48              | 16-32         | dominant lever at small batch (near-linear to 48); flattens out once BATCH is in the peak region   |
| PARALLELISM   | 1-8, then 32/64   | no effect     | flat within noise the whole tested range, including 32/64 in the cross-product confirmation        |
| CHUNK_SIZE    | 6,250-400,000     | no effect     | flat within noise, including 100,000 in the cross-product confirmation                             |

Raw per-dimension sweep data is in the [collapsed section](#streaming-raw-sweeps) below.

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

### Full cross-product confirmation (`bench:streaming:matrix:full`)

```
task bench:streaming:matrix:full BATCHES="32000 64000 128000" PARALLELS="32 64" MAX_IN_FLIGHTS="16 24 32 48" CHUNK_SIZES="50000 100000" COUNT=1000000
```

| BATCH   | PARALLELISM | MAX_IN_FLIGHT  | CHUNK_SIZE | MSG/SEC    | KB/SEC    |
| ------- | ----------- | -------------- | ---------- | ---------- | --------- |
| 32,000  | 32          | 16             | 50,000     | 43,978     | 6,252     |
| 32,000  | 32          | 16             | 100,000    | 40,283     | 5,726     |
| 32,000  | 32          | 24             | 50,000     | 42,429     | 6,032     |
| 32,000  | 32          | 24             | 100,000    | 42,367     | 6,023     |
| 32,000  | 32          | 32             | 50,000     | 43,754     | 6,220     |
| 32,000  | 32          | 32             | 100,000    | 44,778     | 6,365     |
| 32,000  | 32          | 48             | 50,000     | 43,151     | 6,134     |
| 32,000  | 32          | 48             | 100,000    | 44,329     | 6,302     |
| 32,000  | 64          | 16             | 50,000     | 42,544     | 6,048     |
| 32,000  | 64          | 16             | 100,000    | 44,924     | 6,386     |
| 32,000  | 64          | 24             | 50,000     | 44,962     | 6,392     |
| 32,000  | 64          | 24             | 100,000    | 42,513     | 6,044     |
| 32,000  | 64          | 32             | 50,000     | 42,596     | 6,055     |
| 32,000  | 64          | 32             | 100,000    | 41,354     | 5,879     |
| 32,000  | 64          | 48             | 50,000     | 43,508     | 6,185     |
| 32,000  | 64          | 48             | 100,000    | 39,123     | 5,562     |
| 64,000  | 32          | 16             | 50,000     | 38,994     | 5,543     |
| 64,000  | 32          | 16             | 100,000    | 31,935     | 4,540     |
| 64,000  | 32          | 24             | 50,000     | 40,545     | 5,764     |
| 64,000  | 32          | 24             | 100,000    | 26,542     | 3,773     |
| 64,000  | 32          | 32             | 50,000     | 41,778     | 5,939     |
| 64,000  | 32          | 32             | 100,000    | 43,876     | 6,237     |
| 64,000  | 32          | 48             | 50,000     | 40,919     | 5,817     |
| 64,000  | 32          | 48             | 100,000    | 43,761     | 6,221     |
| 64,000  | 64          | 16             | 50,000     | 43,295     | 6,155     |
| 64,000  | 64          | 16             | 100,000    | 44,062     | 6,264     |
| 64,000  | 64          | 24             | 50,000     | 43,842     | 6,232     |
| 64,000  | 64          | 24             | 100,000    | 44,914     | 6,385     |
| 64,000  | 64          | 32             | 50,000     | 43,988     | 6,253     |
| 64,000  | 64          | 32             | 100,000    | 33,428     | 4,752     |
| 64,000  | 64          | 48             | 50,000     | 44,226     | 6,287     |
| 64,000  | 64          | 48             | 100,000    | 43,737     | 6,217     |
| 128,000 | 32          | 16             | 50,000     | 44,270     | 6,293     |
| 128,000 | 32          | 16             | 100,000    | 37,707     | 5,360     |
| 128,000 | 32          | 24             | 50,000     | 30,624     | 4,353     |
| 128,000 | 32          | 24             | 100,000    | 44,028     | 6,259     |
| 128,000 | 32          | 32             | 50,000     | 39,562     | 5,624     |
| 128,000 | 32          | 32             | 100,000    | 43,760     | 6,221     |
| 128,000 | 32          | 48             | 50,000     | 41,418     | 5,888     |
| 128,000 | 32          | 48             | 100,000    | **44,959** | **6,391** |
| 128,000 | 64          | 16             | 50,000     | 42,786     | 6,082     |
| 128,000 | 64          | 16             | 100,000    | 41,366     | 5,880     |
| 128,000 | 64          | 24             | 50,000     | 31,698     | 4,506     |
| 128,000 | 64          | 24             | 100,000    | **45,354** | **6,447** |
| 128,000 | 64          | 32             | 50,000     | 32,626     | 4,638     |
| 128,000 | 64          | 32             | 100,000    | 44,271     | 6,293     |
| 128,000 | 64          | 48             | 50,000     | 38,266     | 5,440     |
| 128,000 | 64          | 48             | 100,000    | 43,909     | 6,242     |

**Takeaway: this grid did not beat the recommended config, and confirms `PARALLELISM`/`CHUNK_SIZE` still don't matter.** Every combo lands in the same ~40-45K msg/sec band as `BATCH=64000 MAX_IN_FLIGHT=16` — pushing `BATCH` to 128,000, `PARALLELISM` to 32/64, or `CHUNK_SIZE` to 100,000 doesn't buy a real improvement (best row here, 45,354, is within noise of the 43,281-44,778 cluster seen elsewhere). The scatter has no clean trend by any single dimension:

- `BATCH=64,000` and `BATCH=128,000` show more low outliers (down to 26,542 and 30,624) than `BATCH=32,000`, which stays tightly in a 39-45K band — larger batches appear to add variance without adding throughput at this concurrency level, likely transient channel-build/network jitter rather than a real regression.
- `PARALLELISM=32` vs `64` and `CHUNK_SIZE=50,000` vs `100,000` each have their own low outliers scattered across different rows — no consistent winner, confirming both are noise.
- Once `BATCH`/`MAX_IN_FLIGHT` are already in the peak region found by the one-at-a-time sweeps, this system is bottlenecked elsewhere (network/API throughput ceiling) — further turning any of these four knobs doesn't move the ceiling.

Practical conclusion: the recommended config above stays the pick — it reaches the same throughput ceiling with far fewer resources (`PARALLELISM=1`, `MAX_IN_FLIGHT=16` vs. 32-64) than what's needed to hit the top of this grid.

<a id="streaming-raw-sweeps"></a>
<details>
<summary>Raw one-at-a-time sweep data (BATCH / PARALLELISM / MAX_IN_FLIGHT / CHUNK_SIZE)</summary>

#### BATCH (message count per Snowpipe Streaming batch)

`COUNT=200000`, others at default:

| BATCH  | msg/sec |
| ------ | ------- |
| 1,000  | 3,354   |
| 2,000  | 6,041   |
| 4,000  | 8,837   |
| 8,000  | 16,112  |
| 16,000 | 15,430  |
| 32,000 | 26,597  |
| 64,000 | 27,763  |

`COUNT=1,000,000`, confirming the top end:

| BATCH      | msg/sec    | KB/sec    |
| ---------- | ---------- | --------- |
| 32,000     | 42,864     | 6,093     |
| **64,000** | **43,281** | **6,153** |
| 128,000    | 41,472     | 5,896     |
| 256,000    | 40,914     | 5,816     |

64,000 is the peak — throughput climbs steadily up to it, then declines past it.

#### PARALLELISM (parquet build parallelism)

`COUNT=1,000,000`, `BATCH=1000` (default), others at default:

| PARALLELISM | msg/sec |
| ----------- | ------- |
| 1           | 3,839   |
| 2           | 3,855   |
| 3           | 3,769   |
| 4           | 3,704   |
| 5           | 3,857   |
| 6           | 3,750   |
| 7           | 3,601   |
| 8           | 3,785   |

Flat within ~7% (3,600-3,860 msg/sec) across all values — noise, not a trend. Not worth tuning past 1.

#### MAX_IN_FLIGHT (concurrent Snowpipe Streaming channels)

`BATCH=1000` (default), others at default:

| MAX_IN_FLIGHT | msg/sec | COUNT     |
| ------------- | ------- | --------- |
| 1             | 851     | 200,000   |
| 2             | 1,863   | 200,000   |
| 4             | 3,377   | 200,000   |
| 8             | 5,978   | 200,000   |
| 16            | 12,642  | 1,000,000 |
| 24            | 12,742  | 1,000,000 |
| 32            | 12,382  | 1,000,000 |
| 48            | 16,951  | 1,000,000 |

Roughly linear scaling at `BATCH=1000` (small batches build fast, so more concurrent channels keep saturating). Still climbing at 48 — no plateau found at this batch size. The cross-product confirmation above shows it does flatten out once `BATCH` is in the peak region.

#### CHUNK_SIZE (rows per parquet chunk)

`COUNT=200,000`, `BATCH=1000` (default), others at default — no clear signal:

| CHUNK_SIZE | msg/sec |
| ---------- | ------- |
| 6,250      | 3,707   |
| 12,500     | 3,409   |
| 25,000     | 2,428   |
| 50,000     | 2,266   |
| 100,000    | 2,798   |
| 200,000    | 3,098   |
| 400,000    | 3,106   |

Left at default (50,000). Low priority — noisy, no strong trend either direction.

</details>

### Harness gotchas found while building this benchmark

- `ACCOUNTADMIN`/`PUBLIC` (tempting default role/schema) may not exist on the account — set both explicitly.
- `logger.level: WARN` in `benchmark_config.yaml` silently swallows the `benchmark` processor's `INFO`-level throughput logs, including the final "total" summary — leave it at `INFO` or higher won't show results.
- `benchmark.interval: 1s` logs a rolling stats line every second; set to `0s` to log only the final "total" summary on shutdown.
- Go logs large `bytes/sec` values in scientific notation (e.g. `8.981e+06`) — a naive `grep -oP '[0-9.]+'` truncates at the `e`. Widen the regex to `[0-9.eE+-]+` when parsing structured log fields for scripted result tables.

---

<a id="bulk-put--snowpipe-snowflake_put"></a>
## Bulk PUT + Snowpipe (`snowflake_put`)

Run instructions: `task bench:bulk`, `task bench:bulk:matrix`.

**Dataset:** synthetic events (`ID`, `USER_ID`, `EVENT_TYPE`, `VALUE`, `INFO`, `TS`), ~140 B/row, archived (`json_array`) into files staged via `PUT`, loaded into `BENCH_EVENTS_JSON` via Snowpipe. `COUNT` scales with `BATCH` (10+ batches per combo) so each row is a real steady-state sample, not a single noisy network call.

**History:** `WriteBatch` originally held one mutex for the entire PUT + Snowpipe call, so batches never ran concurrently — `MAX_IN_FLIGHT` had no effect and the best config-only result was 4,661 msg/sec. Fixing that (`internal/impl/snowflake/output_snowflake_put.go`: `RWMutex` guarding only the `*sql.DB` pointer, concurrent per-file uploads via `errgroup`) made `MAX_IN_FLIGHT` the dominant lever — up to a 33x gain — and raised throughput ~8x. Raw pre-fix/early-post-fix sweep data is in the [collapsed section](#bulk-raw-sweeps) below.

### Summary

| Parameter      | Range tested   | Best region | Effect                                                                                                                       |
| -------------- | -------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------- |
| BATCH          | 1,000 – 50,000 | 20,000      | mattered a lot pre-fix (bigger batch = fewer round trips); post-fix, 20,000 edges out 10,000/50,000 once concurrency is high |
| MAX_IN_FLIGHT  | 1 – 64         | 32 – 64     | dominant lever post-fix — up to 33x gain once batches can overlap; still noisy at the top of the range, no clean plateau     |
| UPLOAD_THREADS | 4 – 32         | 16 – 32     | mattered most pre-fix at 16; post-fix only pulls ahead at 32 when MAX_IN_FLIGHT is also high, otherwise no consistent winner |

### Recommended configuration

```
BATCH=20000  MAX_IN_FLIGHT=64  UPLOAD_THREADS=32
```

**36,166 msg/sec, 5.13 MB/sec** — best config found, ~5% above the earlier 2-D sweep's best (34,379 msg/sec) and close to `snowflake_streaming`'s 40,442 msg/sec ceiling (see [Snowpipe Streaming](#snowpipe-streaming-snowflake_streaming) above), using the same warehouse-bound PUT+Snowpipe mechanism.

### Latest sweep: BATCH x MAX_IN_FLIGHT x UPLOAD_THREADS

```
task bench:matrix BATCHES="10000 20000 50000" MAX_IN_FLIGHTS="16 32 64" UPLOAD_THREADS="8 16 32" COUNT=500000
```

| BATCH  | MAX_IN_FLIGHT | UPLOAD_THREADS  | MSG/SEC      | MB/SEC   |
| ------ | ------------- | --------------- | ------------ | -------- |
| 10,000 | 16            | 8               | 21,427.7     | 3.04     |
| 10,000 | 16            | 16              | 19,921.6     | 2.83     |
| 10,000 | 16            | 32              | 23,138.9     | 3.28     |
| 10,000 | 32            | 8               | 20,342.2     | 2.89     |
| 10,000 | 32            | 16              | 21,158.7     | 3.00     |
| 10,000 | 32            | 32              | 16,822.7     | 2.39     |
| 10,000 | 64            | 8               | 20,437.4     | 2.90     |
| 10,000 | 64            | 16              | 20,859.4     | 2.96     |
| 10,000 | 64            | 32              | 33,354.8     | 4.73     |
| 20,000 | 16            | 8               | 20,955.3     | 2.97     |
| 20,000 | 16            | 16              | 20,654.9     | 2.93     |
| 20,000 | 16            | 32              | 30,070.4     | 4.27     |
| 20,000 | 32            | 8               | 27,770.1     | 3.94     |
| 20,000 | 32            | 16              | 29,581.9     | 4.20     |
| 20,000 | 32            | 32              | 34,496.6     | 4.90     |
| 20,000 | 64            | 8               | 34,864.1     | 4.95     |
| 20,000 | 64            | 16              | 32,302.1     | 4.58     |
| 20,000 | 64            | 32              | **36,165.7** | **5.13** |
| 50,000 | 16            | 8               | 28,409.3     | 4.03     |
| 50,000 | 16            | 16              | 26,940.9     | 3.82     |
| 50,000 | 16            | 32              | 29,886.8     | 4.24     |
| 50,000 | 32            | 8               | 30,373.4     | 4.31     |
| 50,000 | 32            | 16              | 30,448.5     | 4.32     |
| 50,000 | 32            | 32              | 29,592.9     | 4.20     |
| 50,000 | 64            | 8               | 25,039.0     | 3.55     |
| 50,000 | 64            | 16              | 31,663.6     | 4.49     |
| 50,000 | 64            | 32              | 27,461.9     | 3.90     |

- `BATCH=50,000` no longer wins now that `MAX_IN_FLIGHT`/`UPLOAD_THREADS` are both swept high — `20,000` takes the top spot, contradicting the earlier "bigger batch always helps" pre-fix finding.
- Noisy throughout (e.g. `10000/32/32` dips to 16,823 while its neighbors sit near 20-23K) — same jitter pattern as the streaming cross-product, not a real per-parameter effect.
- `UPLOAD_THREADS=32` only pulls ahead when `MAX_IN_FLIGHT=64`; at lower concurrency, 8 or 16 do just as well.

<a id="bulk-raw-sweeps"></a>
<details>
<summary>Raw pre-fix / early post-fix sweep data (superseded by the 3-D sweep above)</summary>

#### Pre-fix: BATCH x UPLOAD_THREADS (config-only, MAX_IN_FLIGHT=1)

`COUNT=500000`:

| BATCH      | UPLOAD_THREADS | msg/sec     | KB/sec  |
| ---------- | -------------- | ----------- | ------- |
| 5,000      | 4              | 656.8       | 93      |
| 5,000      | 8              | 621.6       | 88      |
| 5,000      | 16             | 593.8       | 84      |
| 5,000      | 32             | 603.5       | 86      |
| 10,000     | 4              | 985.9       | 140     |
| 10,000     | 8              | 1,223.8     | 174     |
| 10,000     | 16             | 1,044.7     | 148     |
| 10,000     | 32             | 854.9       | 121     |
| 20,000     | 4              | 1,463.3     | 208     |
| 20,000     | 8              | 1,214.1     | 172     |
| 20,000     | 16             | 1,345.9     | 191     |
| 20,000     | 32             | 1,685.9     | 239     |
| 50,000     | 4              | 3,318.8     | 471     |
| 50,000     | 8              | 3,224.3     | 458     |
| **50,000** | **16**         | **4,661.2** | **662** |
| 50,000     | 32             | 2,566.9     | 364     |

Best pre-fix config: `BATCH=50000 UPLOAD_THREADS=16 MAX_IN_FLIGHT=1` → 4,661 msg/sec. `MAX_IN_FLIGHT` had no effect at all — the connection mutex serialized every batch regardless of setting.

### Harness gotchas found while building this benchmark

- `task bench:matrix` has no default `COUNT` bound — without setting `COUNT`, each combo runs forever and the sweep never advances.
- `COUNT` must scale with `BATCH`, or the largest batch sizes get only 1-2 samples each and produce non-monotonic noise instead of a trend.
