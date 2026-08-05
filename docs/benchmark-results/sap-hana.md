# SAP HANA Benchmark Results

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.114.1), x86_64

See [`internal/impl/saphana/bench/`](../../internal/impl/saphana/bench/) for configs and run instructions.

---

## Bulk Read

Full scan of `BENCH_ORDERS`: 2,000,000 rows × ~300 B (BIGINT, INTEGER × 3, DECIMAL, NVARCHAR(20), NVARCHAR(200), TIMESTAMP).
Pipeline: `sap_hana` input (bulk mode) → `kafka_franz` output. `max_in_flight=10`.
Varying `fetch_size`, `batching.count`, and `GOMAXPROCS`.

### msg/sec

| fetch_size | batch  | cores=1 | cores=2 | cores=4    | cores=8    |
|------------|--------|---------|---------|------------|------------|
| 1,000      | 1,000  | 20,202  | 20,833  | 21,739     | 21,978     |
| 1,000      | 5,000  | 19,231  | 21,739  | 20,408     | 19,417     |
| 1,000      | 10,000 | 18,868  | 19,231  | 19,048     | 19,802     |
| 10,000     | 1,000  | 36,364  | 38,462  | 40,816     | 37,736     |
| 10,000     | 5,000  | 37,736  | 40,000  | 31,250     | 44,444     |
| 10,000     | 10,000 | 32,258  | 41,667  | 42,553     | **48,780** |
| 100,000    | 1,000  | 32,258  | 38,462  | 45,455     | 42,553     |
| 100,000    | 5,000  | 38,462  | 40,000  | 40,000     | 43,478     |
| 100,000    | 10,000 | 37,736  | 45,455  | **46,512** | 38,462     |

### Best result per fetch_size (across all batch and core counts)

| fetch_size | Best msg/s | Config                    |
|------------|------------|---------------------------|
| 1,000      | 21,978     | batch=1000, cores=8       |
| 10,000     | **48,780** | batch=10000, cores=8      |
| 100,000    | 46,512     | batch=10000, cores=4      |

**Observations:**
- `fetch_size` is the dominant parameter. Increasing from 1,000 to 10,000 roughly doubles throughput by reducing HANA `FetchNext` round-trips for 2M rows from ~2,000 to ~200.
- Increasing `fetch_size` from 10,000 to 100,000 yields marginal gains; the bottleneck shifts from HANA network to Kafka produce and Go processing.
- Kafka `batching.count` has secondary effect. Larger batches reduce Kafka produce round-trips but do not compensate for a small `fetch_size`.
- Core scaling is weak because the pipeline is dominated by sequential HANA cursor reads (single connection, single result set). Extra goroutines help overlap Kafka I/O with HANA processing but saturate quickly.
- **Recommended configuration: `fetch_size=10000`, `batching.count=10000`, `GOMAXPROCS=8` → ~49,000 msg/s (~2M rows in 41s).**

---

## Bulk Read — Kafka Connect JDBC Source (comparison)

Same dataset: 2,000,000 rows × ~300 B via `io.confluent.connect.jdbc.JdbcSourceConnector` (mode=bulk).
`tasks.max=1`. Varying `jdbc.fetch.size` and `batch.max.rows`.

### msg/sec

| fetch_size | batch=1,000 | batch=5,000 | batch=10,000 |
|------------|-------------|-------------|--------------|
| 1,000      | 15,748      | 54,054      | 76,923       |
| 10,000     | 15,385      | 55,556      | **86,957**   |
| 100,000    | 15,873      | 60,606      | 83,333       |

**Observations:**
- `batch.max.rows` dominates: `batch=1000` caps at ~16k msg/s regardless of `fetch_size`; `batch=10000` reaches ~87k msg/s.
- `fetch_size` has negligible effect — the JDBC connector buffers rows internally and the bottleneck is how many rows are published per Kafka write, not how many rows are fetched per HANA round-trip.
- Peak 86,957 msg/s (`fetch_size=10000`, `batch=10000`) exceeds the `sap_hana` bulk read peak (~49k msg/s) at the cost of no schema metadata, no incremental capture, and Java/JVM overhead.

---

## Incrementing Read

Concurrent load + capture: 500,000 rows inserted via 10 parallel workers while the connector polls for new rows.
Pipeline: `sap_hana` input (incrementing mode, `incrementing_column=ID`) → `kafka_franz` output. `max_in_flight=10`, `batching.count=1000`.
Varying `fetch_size`, `GOMAXPROCS`, and `poll_interval`.

### msg/sec

| fetch_size | poll   | cores=1 | cores=2 | cores=4    | cores=8    |
|------------|--------|---------|---------|------------|------------|
| 1,000      | 100ms  | 20,000  | 20,000  | 21,739     | 22,727     |
| 1,000      | 500ms  | 20,000  | 20,000  | 17,857     | 19,231     |
| 1,000      | 1s     | 20,000  | 20,000  | 20,000     | 19,231     |
| 10,000     | 100ms  | 31,250  | 31,250  | 38,462     | 38,462     |
| 10,000     | 500ms  | 31,250  | 38,462  | 38,462     | **41,667** |
| 10,000     | 1s     | 26,316  | 31,250  | 31,250     | 38,462     |
| 100,000    | 100ms  | 31,250  | 38,462  | 38,462     | 22,727     |
| 100,000    | 500ms  | 26,316  | 38,462  | 31,250     | 38,462     |
| 100,000    | 1s     | 15,625  | 20,000  | 38,462     | 38,462     |

### Best result per fetch_size (across all poll and core counts)

| fetch_size | Best msg/s | Config                          |
|------------|------------|---------------------------------|
| 1,000      | 22,727     | poll=100ms, cores=8             |
| 10,000     | **41,667** | poll=500ms, cores=8             |
| 100,000    | 38,462     | poll=100ms/500ms, cores=2/4/8   |

**Observations:**
- `fetch_size` is again the dominant parameter. Increasing from 1,000 to 10,000 roughly doubles throughput (~22k → ~42k msg/s).
- `fetch_size=100000` does not improve over 10,000 and is slightly slower in some configurations due to larger result-set transfer per poll.
- `poll_interval=100ms` performs best at `fetch_size=1000` where more frequent polls compensate for small batches. At larger fetch sizes `poll_interval` has less effect since each poll already returns a large batch.
- Core scaling is modest; the bottleneck is HANA cursor read latency, not CPU.
- **Recommended configuration: `fetch_size=10000`, `poll_interval=100ms–500ms`, `GOMAXPROCS=4–8` → ~38,000–42,000 msg/s.**

---

## Incrementing Read — Kafka Connect JDBC Source (comparison)

Same concurrent load + capture: 500,000 rows inserted via 10 parallel workers while the connector polls.
`io.confluent.connect.jdbc.JdbcSourceConnector` (`mode=incrementing`, `incrementing.column.name=ID`). `tasks.max=1`.
Varying `poll.interval.ms` and `batch.max.rows`.

### msg/sec

| batch  | poll   | msg/s      |
|--------|--------|------------|
| 1,000  | 100ms  | 13,158     |
| 1,000  | 500ms  | 12,821     |
| 1,000  | 1s     | 13,158     |
| 5,000  | 100ms  | 35,714     |
| 5,000  | 500ms  | **41,667** |
| 5,000  | 1s     | 38,462     |
| 10,000 | 100ms  | 38,462     |
| 10,000 | 500ms  | 31,250     |
| 10,000 | 1s     | 38,462     |

**Observations:**
- `batch.max.rows` is the dominant parameter. Default (100) yields ~1,700 msg/s; setting it to 5,000–10,000 reaches ~42k msg/s — a 24× improvement.
- `poll.interval.ms` has minimal effect once `batch.max.rows` is large enough; each poll already drains all available rows.
- Peak 41,667 msg/s (`batch=5000`, `poll=500ms`) matches `sap_hana` peak (~42k msg/s) at equivalent load.
- KC JDBC incrementing with correct `batch.max.rows` is competitive with `sap_hana` incrementing mode, unlike the out-of-box default.

---

## Query Read

Full scan via user-supplied SQL: 2,000,000 rows × ~300 B (BIGINT, INTEGER × 3, DECIMAL, NVARCHAR(20), NVARCHAR(200), TIMESTAMP).
Pipeline: `sap_hana` input (query mode) → `kafka_franz` output. `max_in_flight=10`.
Query: `SELECT * FROM "SCHEMA"."BENCH_ORDERS_QUERY"`. Varying `fetch_size` and `GOMAXPROCS`.

### msg/sec

| fetch_size | cores=1 | cores=2 | cores=4    | cores=8 |
|------------|---------|---------|------------|---------|
| 1,000      | 22,727  | 22,222  | 21,505     | 23,529  |
| 10,000     | 68,966  | 76,923  | 76,923     | 71,429  |
| 100,000    | 62,500  | 68,966  | **95,238** | 90,909  |

### Best result per fetch_size (across all core counts)

| fetch_size | Best msg/s | Config        |
|------------|------------|---------------|
| 1,000      | 23,529     | cores=8       |
| 10,000     | 76,923     | cores=2 or 4  |
| 100,000    | **95,238** | cores=4       |

**Observations:**
- `fetch_size` is again the dominant parameter. Increasing from 1,000 to 10,000 roughly triples throughput (~23k → ~77k msg/s).
- Unlike bulk mode, `fetch_size=100,000` outperforms 10,000 at higher core counts (~95k vs ~77k msg/s). Query mode does not iterate a server-side cursor between fetches; a larger fetch size directly reduces HANA round-trips per result set.
- Core scaling is more effective at `fetch_size=100,000`: cores=4 achieves the overall peak, suggesting that larger result transfers benefit from more parallel Kafka produce capacity.
- **Recommended configuration: `fetch_size=100000`, `GOMAXPROCS=4` → ~95,000 msg/s (~2M rows in 21s).**

---

## Query Read — Kafka Connect JDBC Source (comparison)

Same dataset: 2,000,000 rows × ~300 B via `io.confluent.connect.jdbc.JdbcSourceConnector` (`mode=bulk`, custom query with `CAST(PRICE AS DOUBLE)`).
`tasks.max=1`, `poll.interval.ms=86400000` (one-shot). Varying `jdbc.fetch.size` and `batch.max.rows`.

### msg/sec

| fetch_size | batch=1,000 | batch=5,000 | batch=10,000   |
|------------|-------------|-------------|----------------|
| 1,000      | 16,129      | 62,500      | **90,909**     |
| 10,000     | 15,873      | 57,143      | 76,923         |
| 100,000    | 15,748      | 55,556      | 76,923         |

**Observations:**
- `batch.max.rows` dominates: `batch=1000` caps at ~16k msg/s; `batch=10000` reaches ~91k msg/s regardless of `fetch_size`.
- `fetch_size` has minimal effect — the JDBC driver buffers rows internally and the bottleneck is Kafka produce batch size, not HANA fetch round-trips.
- Peak 90,909 msg/s (`fetch_size=1000`, `batch=10000`) is slightly below `sap_hana` query peak (~95k msg/s), which benefits from larger `fetch_size` reducing HANA round-trips.
- Unlike `sap_hana` query mode, increasing `fetch_size` beyond 1,000 does not help KC and slightly regresses at `batch=10000`.

---

## Write

Kafka → `sap_hana` output (native bulk insert): 2,000,000 rows × 4 columns (BIGINT, NVARCHAR(50), DOUBLE, TIMESTAMP).
Pipeline: `kafka_franz` input → `sap_hana` output. Each batch is sent via a cached prepared statement; go-hdb batches all rows into a single `MtInsert` RPC.
`batching.count=1000`. Varying `max_in_flight` (concurrent batch INSERT calls) and `GOMAXPROCS`.

### msg/sec

| max_in_flight | cores=1 | cores=2 | cores=4 | cores=8    |
|---------------|---------|---------|---------|------------|
| 5             | 31,250  | 34,483  | 36,364  | 36,364     |
| 10            | 28,986  | 51,282  | 40,816  | **57,143** |
| 20            | 28,169  | 33,333  | 35,714  | 37,736     |
| 50            | 15,873  | 18,868  | 22,989  | 19,231     |

### Best result per max_in_flight (across all core counts)

| max_in_flight | Best msg/s | Config       |
|---------------|------------|--------------|
| 5             | 36,364     | cores=4 or 8 |
| 10            | **57,143** | cores=8      |
| 20            | 37,736     | cores=8      |
| 50            | 22,989     | cores=4      |

**Observations:**
- `max_in_flight=10` is the sweet spot: ~57k msg/s at 8 cores. Lower values under-saturate HANA; higher values cause contention.
- `max_in_flight=50` degrades significantly (~23k msg/s peak) — too many concurrent INSERT RPCs overwhelm HANA's concurrency handling.
- Core scaling is most effective at `max_in_flight=10`: throughput nearly doubles from cores=1 to cores=8 (~29k → ~57k msg/s).
- At `max_in_flight=5` and `max_in_flight=20`, gains plateau above cores=4, suggesting the bottleneck shifts to HANA INSERT latency.
- **Recommended configuration: `max_in_flight=10`, `batching.count=1000`, `GOMAXPROCS=8` → ~57,000 msg/s (~2M rows in 35s).**

---

## Write — Kafka Connect SAP HANA Sink (comparison)

Kafka → `com.sap.kafka.connect.sink.hana.HANASinkConnector` (kafka-connect-sap 0.9.4): 2,000,000 rows × 3 columns (BIGINT, NVARCHAR(50), DOUBLE).
Schema-embedded JSON via `org.apache.kafka.connect.json.JsonConverter`. `consumer.override.max.poll.records` set to match `batch.size`.
Varying `batch.size` (rows per JDBC `executeBatch` call) and `tasks.max` (parallel Kafka consumer/writer tasks).

### msg/sec

| batch_size | tasks=1    | tasks=2    | tasks=4    |
|------------|------------|------------|------------|
| 1,000      | 6,645      | 6,579      | 6,601      |
| 5,000      | 20,408     | 20,408     | 20,408     |
| 10,000     | 28,986     | 28,986     | 28,986     |

**Observations:**
- `batch_size` dominates throughput; `tasks.max` has no effect — HANA JDBC insert throughput is single-threaded bounded.
- `batch_size=10,000` peaks at ~29k msg/s (~2M rows in 69s). Larger batches likely continue to improve up to HANA JDBC limits.
- `sap_hana` native output (go-hdb `execMany`) achieves **~2× higher throughput** (~57k msg/s) vs KC JDBC Sink (~29k msg/s) at equivalent batch sizes, using a single RPC rather than JDBC `executeBatch`.
- `consumer.override.max.poll.records` must match `batch.size`; without it, KC Kafka consumer caps at 500 records/poll and `batch.size` has no effect.

---

## EC2 Testing

**Environment:** AWS EC2 instance, Intel Xeon Platinum 8488C, 1 socket, **4 physical
cores × 2 threads (hyperthreading) = 8 vCPUs** (`nproc` = 8), single NUMA node, 105 MiB
shared L3. Note: `CORES=8` in these benchmarks means all 8 *logical* threads, i.e.
oversubscribing the 4 physical cores 2x — this likely explains why `CORES=8` plateaus
or regresses vs `CORES=4` across every bench below (hyperthread contention, not true
additional parallelism). `CORES=4` is the real "use all physical cores" setting.

### Bulk Read (draft)

> Exploratory pass — goal was to see which parameters move the needle before running a
> proper/final benchmark. `BATCH` (Kafka produce batch) turned out to have negligible
> effect; `FETCH_SIZE` is the dominant factor, with `CORES` mattering once `FETCH_SIZE`
> is large enough to become CPU-bound. Proper benchmark results (narrower, more
> representative sweep) will be added below once run.

1,000,000 rows, `max_in_flight=10`.

### msg/sec

| fetch_size | batch  | cores=1 | cores=2 | cores=4    | cores=8    |
|------------|--------|---------|---------|------------|------------|
| 1,000      | 1,000  | 8,130   | 8,264   | 8,264      | 8,264      |
| 1,000      | 5,000  | 7,937   | 8,264   | 8,264      | 8,333      |
| 1,000      | 10,000 | 8,000   | 8,264   | 8,333      | 8,264      |
| 10,000     | 1,000  | 40,000  | 50,000  | 47,619     | 50,000     |
| 10,000     | 5,000  | 40,000  | 50,000  | 50,000     | 45,455     |
| 10,000     | 10,000 | 41,667  | 47,619  | 50,000     | 50,000     |
| 100,000    | 1,000  | 38,462  | 58,824  | 66,667     | **71,429** |
| 100,000    | 5,000  | 35,714  | 58,824  | **71,429** | 58,824     |
| 100,000    | 10,000 | 38,462  | 55,556  | 66,667     | 66,667     |

### Best result per fetch_size (across all batch and core counts)

| fetch_size | Best msg/s | Config               |
|------------|------------|-----------------------|
| 1,000      | 8,333      | batch=5000, cores=8   |
| 10,000     | 50,000     | batch=1000, cores=2   |
| 100,000    | **71,429** | batch=1000, cores=8   |

**Takeaway:** `BATCH` sweep dropped from the follow-up matrix (no measurable effect).
Next pass narrows to `FETCH_SIZE` × `CORES` only, with `FETCH_SIZE` pushed higher
(200000, 500000) to see where throughput plateaus.

### Bulk Read

`BATCH_COUNT` fixed at 5000 (confirmed no effect in the draft pass). `TOTAL` raised to
5,000,000 rows (10× the largest `FETCH_SIZE`) so every config gets several fetches
instead of completing in one or two.

```bash
task bench:load COUNT=5000000
task bench:matrix FETCH="1000 10000 100000 200000 500000" BATCH=5000 CORES="1 2 4 8" OUT=results_v2.txt
```

5,000,000 rows, `batching.count=5000` fixed (confirmed no effect in the draft pass).

### msg/sec

| fetch_size | cores=1 | cores=2 | cores=4    | cores=8    |
|------------|---------|---------|------------|------------|
| 1,000      | 7,886   | 8,389   | 8,375      | 8,361      |
| 10,000     | 44,248  | 54,348  | 56,180     | 56,180     |
| 100,000    | 41,322  | 73,529  | **83,333** | 61,728     |
| 200,000    | 42,735  | 74,627  | 70,423     | 66,667     |
| 500,000    | 41,322  | 68,493  | 81,967     | **83,333** |

### Best result per fetch_size (across all core counts)

| fetch_size | Best msg/s | Config  |
|------------|------------|---------|
| 1,000      | 8,389      | cores=2 |
| 10,000     | 56,180     | cores=4 or 8 |
| 100,000    | **83,333** | cores=4 |
| 200,000    | 74,627     | cores=2 |
| 500,000    | **83,333** | cores=8 |

**Takeaway:**
- `FETCH_SIZE=1000` is the clear bottleneck (~8k msg/s, network/round-trip bound) —
  avoid in production configs.
- `FETCH_SIZE≥100000` plateaus around 41-44k msg/s at `CORES=1`, then scales with
  cores up to a **best result of ~81-83k msg/s at `CORES=4`** (fetch=100000 or 500000).
- `CORES=8` does not consistently beat `CORES=4` — at fetch=100000/200000 it's actually
  *worse* (61-67k vs 70-83k), suggesting oversubscription/contention past 4 cores on
  this 8-vCPU box.
- Pushing `FETCH_SIZE` beyond 100000 (to 200000, 500000) gives no further gain —
  100000 is effectively the plateau point.
- **Recommended config: `FETCH_SIZE=100000, CORES=4`** (~83k msg/s).

### Incrementing Read

```bash
task bench:matrix COUNT=500000 OUT=rpcn_inc.txt
```

500,000 rows.

### msg/sec

| fetch_size | poll  | cores=1 | cores=2 | cores=4 | cores=8 |
|------------|-------|---------|---------|---------|---------|
| 1,000      | 100ms | 7,576   | 7,576   | 7,692   | 7,576   |
| 1,000      | 500ms | 7,692   | 7,246   | 7,692   | 7,692   |
| 1,000      | 1s    | 5,380   | 7,353   | 7,246   | 7,353   |
| 10,000     | 100ms | 12,073  | 11,667  | 11,667  | 27,778  |
| 10,000     | 500ms | 23,810  | 11,786  | 29,412  | 12,073  |
| 10,000     | 1s    | 10,312  | 23,810  | 23,810  | 27,778  |
| 100,000    | 100ms | 11,951  | **33,333** | 12,692 | 27,778  |
| 100,000    | 500ms | 23,810  | 10,426  | 12,073  | 11,786  |
| 100,000    | 1s    | 23,810  | 27,778  | 27,778  | 27,778  |

### Best result per fetch_size (across all poll and core counts)

| fetch_size | Best msg/s | Config              |
|------------|------------|----------------------|
| 1,000      | 7,692      | poll=500ms, cores=4/8 |
| 10,000     | 29,412     | poll=500ms, cores=4  |
| 100,000    | **33,333** | poll=100ms, cores=2  |

**Takeaway:**
- Same pattern as bulk read: `FETCH_SIZE=1000` bottlenecks throughput at ~7-8k msg/s
  regardless of cores/poll interval. `FETCH_SIZE≥10000` roughly doubles-to-quadruples it.
- `POLL_INTERVAL` shows no clean trend — best and worst results both appear across all
  three intervals (100ms/500ms/1s) at the same `FETCH_SIZE`/`CORES`. Results are noisy
  run-to-run (likely HWM-poll timing/network variance); don't read too much into single
  runs here — worth repeating the top candidates 2-3x before trusting a specific number.
- A handful of rows show `TOTAL` a few thousand short of 500000 (e.g. 495000, 490000).
  This looks like a harness/timing artifact of the polling-based completion check
  rather than confirmed data loss — worth verifying actual row count in the destination
  before treating it as real, the same way `sql_insert` write bench turned out to have
  a premature-completion bug earlier.
- Best observed: `FETCH_SIZE=100000, CORES=2, POLL=100ms` at 33333 msg/s, but given the
  noise above, treat as a candidate to confirm rather than a settled result.

#### Incrementing Read — 5M scale

```bash
task bench:matrix COUNT=5000000 FETCH="10000 100000 500000" CORES="2 4 8" POLL="100ms 500ms" OUT=rpcn_inc_v2.txt
```

5,000,000 rows.

### msg/sec

| fetch_size | poll  | cores=2 | cores=4    | cores=8    |
|------------|-------|---------|------------|------------|
| 10,000     | 100ms | 38,721  | 39,563     | 44,643     |
| 10,000     | 500ms | 37,803  | **47,619** | 34,618     |
| 100,000    | 100ms | 37,803  | 37,689     | 39,643     |
| 100,000    | 500ms | 35,571  | 40,528     | **48,544** |
| 500,000    | 100ms | 36,852  | 39,603     | 40,163     |
| 500,000    | 500ms | 38,566  | 31,955     | 37,519     |

### Best result per fetch_size (across all poll and core counts)

| fetch_size | Best msg/s | Config              |
|------------|------------|----------------------|
| 10,000     | 47,619     | poll=500ms, cores=4 |
| 100,000    | **48,544** | poll=500ms, cores=8 |
| 500,000    | 40,163     | poll=100ms, cores=8 |

All rows hit the full 5,000,000 target. Earlier runs at this scale undershot due to a
harness completion-detection bug (`bench:run` in `Taskfile.yaml:191`, incrementing,
declared the run "done" after Kafka's count was idle for only 8 consecutive 2s ticks —
16s — too short for this scale/fetch size). Threshold raised to 60 ticks (120s); results
above are from the re-run with the patched Taskfile.

### Query Read (draft, 500k)

```bash
task bench:matrix OUT=rpcn_query.txt
```

500,000 rows.

### msg/sec

| fetch_size | cores=1 | cores=2 | cores=4    | cores=8 |
|------------|---------|---------|------------|---------|
| 1,000      | 8,197   | 8,197   | 8,333      | 8,197   |
| 10,000     | 45,455  | 50,000  | 45,455     | 50,000  |
| 100,000    | 55,556  | **71,429** | **71,429** | 62,500  |

### Best result per fetch_size (across all core counts)

| fetch_size | Best msg/s | Config       |
|------------|------------|---------------|
| 1,000      | 8,333      | cores=4       |
| 10,000     | 50,000     | cores=2 or 8  |
| 100,000    | **71,429** | cores=2 or 4  |

**Takeaway:** Same shape as bulk/incrementing — `FETCH_SIZE=1000` bottlenecks at ~8k
msg/s, jumps to ~45-50k at fetch=10000, best at fetch=100000/cores=2-4 (~71k msg/s).
`CORES` beyond 2-4 gives no further gain (8 is flat-to-worse, same pattern as bulk
read). Draft only — 500k rows isn't enough to separate fetch=100000 from a
hypothetical fetch=500000 at this scale; follow-up below runs at 5M.

Follow-up matrix at 5M scale:

```bash
task bench:load COUNT=5000000
task bench:matrix FETCH="10000 100000 500000" CORES="2 4 8" OUT=rpcn_query_v2.txt
```

5,000,000 rows.

### msg/sec

| fetch_size | cores=2 | cores=4     | cores=8    |
|------------|---------|-------------|------------|
| 10,000     | 56,180  | 56,180      | 56,818     |
| 100,000    | 94,340  | **100,000** | 96,154     |
| 500,000    | 87,719  | 96,154      | **98,039** |

### Best result per fetch_size (across all core counts)

| fetch_size | Best msg/s  | Config  |
|------------|-------------|---------|
| 10,000     | 56,818      | cores=8 |
| 100,000    | **100,000** | cores=4 |
| 500,000    | 98,039      | cores=8 |

**Takeaway:**
- All rows hit the full 5,000,000 — clean run, no completion-detection issues here.
- `FETCH_SIZE=10000` plateaus around ~56k msg/s regardless of cores.
- `FETCH_SIZE≥100000` jumps to ~88-100k msg/s — best single result **100000/cores=4 at
  100000 msg/s**, with 500000/cores=8 close behind at 98039.
- `CORES` scaling is flat past 2-4 at this fetch range (2 vs 4 vs 8 all within ~10% of
  each other) — no strong win from pushing cores to 8.
- **Recommended config: `FETCH_SIZE=100000, CORES=4`** (~100k msg/s) — matches the
  bulk read recommendation almost exactly, consistent story across both read modes.
