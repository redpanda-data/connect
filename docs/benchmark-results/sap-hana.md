# SAP HANA Benchmark Results

Throughput comparison of three ways to move data between Kafka and SAP HANA:

| Connector | What it is | Modes tested |
|---|---|---|
| **`sap_hana`** | Redpanda Connect native input/output (go-hdb driver) | Bulk, Incrementing, Query, Write |
| **`kafka-connect-sap`** | SAP's own Kafka Connect connector ([SAP/kafka-connect-sap](https://github.com/SAP/kafka-connect-sap)) | Bulk, Incrementing, Query (EC2 only), Write (local only) |
| **Generic Confluent JDBC** | `io.confluent.connect.jdbc.JdbcSourceConnector` + SAP HANA JDBC driver (`ngdbc.jar`) | Bulk, Incrementing, Query |

See [`internal/impl/saphana/bench/`](../../internal/impl/saphana/bench/) for configs and run instructions.

Write throughput was only benchmarked locally — EC2 write benchmarking is out of scope for this round.

---

## Environments

**Local (WSL2):** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.114.1), x86_64.

**EC2:** AWS EC2, Intel Xeon Platinum 8488C, 1 socket, 4 physical cores × 2 threads (hyperthreading) = 8 vCPUs (`nproc`=8), single NUMA node, 105 MiB shared L3.

> `CORES=8` in EC2 benchmarks means all 8 *logical* threads — oversubscribing the 4 physical cores 2×. This is why `CORES=8` plateaus or regresses vs `CORES=4` across nearly every EC2 sweep below (hyperthread contention, not real extra parallelism). `CORES=4` is "use all physical cores."

HANA reached from EC2 over a network hop measured **~118ms** TCP connect, vs **~70ms** from the local/VPN path used for WSL2 numbers.

---

## Architecture note: why the connectors differ so much

The single biggest factor across every result below is **whether the connector holds a server-side cursor**:

- **`sap_hana`** (go-hdb) and **generic Confluent JDBC** stream one cursor for the whole run — each fetch just pulls the next page from an already-open result set.
- **`kafka-connect-sap`** re-executes `SELECT ... LIMIT <batch.max.rows> OFFSET <running_total>` fresh on *every single poll* (confirmed from `HANAJdbcClient.executeQuery` in its source) — no cursor is held open. Each poll pays full query re-planning + an `OFFSET` skip-scan (cost grows as the offset grows) + a network round trip. This is architectural to the connector, not a config problem.

This explains the ~10× gap between `kafka-connect-sap` and the other two connectors on EC2 (where network RTT amplifies the per-poll penalty), and why `kafka-connect-sap`'s numbers get *worse*, not better, at larger row counts.

---

## Summary — peak throughput (msg/s)

### Local (WSL2), 2M rows unless noted

| Mode | `sap_hana` native | `kafka-connect-sap` | Generic Confluent JDBC |
|---|---|---|---|
| Bulk Read | 48,780 | *not tested* | **86,957** |
| Incrementing Read (500k, concurrent load) | 41,667 | *not tested* | 41,667 |
| Query Read | **95,238** | *not tested* | 90,909 |

### EC2

| Mode | `sap_hana` native | `kafka-connect-sap` | Generic Confluent JDBC |
|---|---|---|---|
| Bulk Read (5M) | **83,333** | 4,255 (200k) | 41,667 (2M) |
| Incrementing Read (5M) | **48,544** | 4,310 (500k) | 35,088 (2M) |
| Query Read (5M) | **100,000** | 4,255 (200k) | 40,000 (2M) |

`sap_hana` and generic Confluent JDBC numbers above are at larger scale (5M/2M) than `kafka-connect-sap` (200k) — scale differences don't change the qualitative story (held-cursor connectors win by ~10× on EC2) but keep the scale gap in mind when quoting exact ratios.

---

## Local (WSL2) — `sap_hana` native

### Bulk Read

Full scan of `BENCH_ORDERS`: 2,000,000 rows × ~300 B (BIGINT, INTEGER × 3, DECIMAL, NVARCHAR(20), NVARCHAR(200), TIMESTAMP).
Pipeline: `sap_hana` input (bulk mode) → `kafka_franz` output. `max_in_flight=10`.
Varying `fetch_size`, `batching.count`, and `GOMAXPROCS`.

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

**Best per fetch_size:** 1,000 → 21,978 (batch=1000, cores=8) · 10,000 → **48,780** (batch=10000, cores=8) · 100,000 → 46,512 (batch=10000, cores=4)

**Observations:**
- `fetch_size` is the dominant parameter — 1,000 → 10,000 roughly doubles throughput by cutting HANA `FetchNext` round-trips for 2M rows from ~2,000 to ~200.
- 10,000 → 100,000 yields marginal gains; bottleneck shifts from HANA network to Kafka produce and Go processing.
- `batching.count` has secondary effect — reduces Kafka produce round-trips but can't compensate for a small `fetch_size`.
- Core scaling is weak — sequential HANA cursor reads (single connection, single result set) dominate; extra goroutines only help overlap Kafka I/O.
- **Recommended: `fetch_size=10000`, `batching.count=10000`, `GOMAXPROCS=8` → ~49,000 msg/s (~2M rows in 41s).**

### Incrementing Read

Concurrent load + capture: 500,000 rows inserted via 10 parallel workers while the connector polls for new rows.
Pipeline: `sap_hana` input (incrementing mode, `incrementing_column=ID`) → `kafka_franz` output. `max_in_flight=10`, `batching.count=1000`.
Varying `fetch_size`, `GOMAXPROCS`, `poll_interval`.

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

**Best per fetch_size:** 1,000 → 22,727 (poll=100ms, cores=8) · 10,000 → **41,667** (poll=500ms, cores=8) · 100,000 → 38,462 (poll=100/500ms, cores=2/4/8)

**Observations:**
- `fetch_size` dominates again — 1,000 → 10,000 roughly doubles throughput (~22k → ~42k).
- `fetch_size=100000` doesn't beat 10,000, sometimes slightly slower (larger result-set transfer per poll).
- `poll_interval=100ms` wins at `fetch_size=1000` (frequent polls compensate small batches); effect shrinks as fetch size grows.
- Core scaling is modest — bottleneck is HANA cursor read latency, not CPU.
- **Recommended: `fetch_size=10000`, `poll_interval=100–500ms`, `GOMAXPROCS=4–8` → ~38,000–42,000 msg/s.**

### Query Read

Full scan via user-supplied SQL: 2,000,000 rows × ~300 B. Pipeline: `sap_hana` input (query mode) → `kafka_franz` output. `max_in_flight=10`.
Query: `SELECT * FROM "SCHEMA"."BENCH_ORDERS_QUERY"`. Varying `fetch_size`, `GOMAXPROCS`.

| fetch_size | cores=1 | cores=2 | cores=4    | cores=8 |
|------------|---------|---------|------------|---------|
| 1,000      | 22,727  | 22,222  | 21,505     | 23,529  |
| 10,000     | 68,966  | 76,923  | 76,923     | 71,429  |
| 100,000    | 62,500  | 68,966  | **95,238** | 90,909  |

**Best per fetch_size:** 1,000 → 23,529 (cores=8) · 10,000 → 76,923 (cores=2 or 4) · 100,000 → **95,238** (cores=4)

**Observations:**
- `fetch_size` dominant — 1,000 → 10,000 roughly triples throughput (~23k → ~77k).
- Unlike bulk mode, `fetch_size=100,000` beats 10,000 at higher core counts (~95k vs ~77k) — query mode doesn't iterate a server-side cursor between fetches the same way, so a bigger fetch directly cuts HANA round-trips.
- Core scaling is more effective here: cores=4 hits the overall peak.
- **Recommended: `fetch_size=100000`, `GOMAXPROCS=4` → ~95,000 msg/s (~2M rows in 21s).**

### Write

Kafka → `sap_hana` output (native bulk insert): 2,000,000 rows × 4 columns (BIGINT, NVARCHAR(50), DOUBLE, TIMESTAMP).
Pipeline: `kafka_franz` input → `sap_hana` output. Each batch sent via a cached prepared statement; go-hdb batches all rows into a single `MtInsert` RPC.
`batching.count=1000`. Varying `max_in_flight` (concurrent batch INSERT calls), `GOMAXPROCS`.

| max_in_flight | cores=1 | cores=2 | cores=4 | cores=8    |
|---------------|---------|---------|---------|------------|
| 5             | 31,250  | 34,483  | 36,364  | 36,364     |
| 10            | 28,986  | 51,282  | 40,816  | **57,143** |
| 20            | 28,169  | 33,333  | 35,714  | 37,736     |
| 50            | 15,873  | 18,868  | 22,989  | 19,231     |

**Best per max_in_flight:** 5 → 36,364 (cores=4/8) · 10 → **57,143** (cores=8) · 20 → 37,736 (cores=8) · 50 → 22,989 (cores=4)

**Observations:**
- `max_in_flight=10` is the sweet spot — ~57k at 8 cores. Lower under-saturates HANA; higher causes contention.
- `max_in_flight=50` degrades badly (~23k peak) — too many concurrent INSERT RPCs overwhelm HANA's concurrency handling.
- Core scaling strongest at `max_in_flight=10` — nearly doubles cores=1→8 (~29k → ~57k).
- **Recommended: `max_in_flight=10`, `batching.count=1000`, `GOMAXPROCS=8` → ~57,000 msg/s (~2M rows in 35s).**

---

## Local (WSL2) — Generic Confluent JDBC (comparison)

`io.confluent.connect.jdbc.JdbcSourceConnector` + SAP HANA JDBC driver, same datasets/loads as the `sap_hana` sections above. `tasks.max=1`.

### Bulk Read

Varying `jdbc.fetch.size`, `batch.max.rows`.

| fetch_size | batch=1,000 | batch=5,000 | batch=10,000 |
|------------|-------------|-------------|--------------|
| 1,000      | 15,748      | 54,054      | 76,923       |
| 10,000     | 15,385      | 55,556      | **86,957**   |
| 100,000    | 15,873      | 60,606      | 83,333       |

**Observations:**
- `batch.max.rows` dominates: `batch=1000` caps at ~16k regardless of fetch size; `batch=10000` reaches ~87k.
- `fetch_size` has negligible effect — connector buffers rows internally; bottleneck is rows-per-Kafka-write, not rows-per-HANA-fetch.
- Peak 86,957 msg/s (fetch=10000, batch=10000) exceeds `sap_hana` bulk peak (~49k) — at the cost of no schema metadata, no incremental capture, and JVM overhead.

### Incrementing Read

Same concurrent load (500k rows / 10 workers). `mode=incrementing`, `incrementing.column.name=ID`, `tasks.max=1`. Varying `poll.interval.ms`, `batch.max.rows`.

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
- `batch.max.rows` dominant — default (100) gives ~1,700 msg/s; 5,000–10,000 reaches ~42k, a 24× improvement.
- `poll.interval.ms` matters little once batch is large enough — each poll already drains available rows.
- Peak 41,667 msg/s (batch=5000, poll=500ms) matches `sap_hana` incrementing peak — competitive here, unlike EC2 where `kafka-connect-sap` (the *other* JDBC connector) falls far behind.

### Query Read

Custom query with `CAST(PRICE AS DOUBLE)`, `mode=bulk`, `tasks.max=1`, `poll.interval.ms=86400000` (one-shot). Varying `jdbc.fetch.size`, `batch.max.rows`.

| fetch_size | batch=1,000 | batch=5,000 | batch=10,000   |
|------------|-------------|-------------|----------------|
| 1,000      | 16,129      | 62,500      | **90,909**     |
| 10,000     | 15,873      | 57,143      | 76,923         |
| 100,000    | 15,748      | 55,556      | 76,923         |

**Observations:**
- `batch.max.rows` dominates: batch=1000 caps ~16k; batch=10000 reaches ~91k regardless of fetch size.
- `fetch_size` has minimal effect — driver buffers internally; bottleneck is Kafka produce batch, not HANA fetch round-trips.
- Peak 90,909 msg/s (fetch=1000, batch=10000) is slightly below `sap_hana` query peak (~95k). Unlike `sap_hana`, increasing fetch size beyond 1,000 doesn't help and slightly regresses at batch=10000.

---

## Local (WSL2) — `kafka-connect-sap` (comparison)

### Write

Kafka → `com.sap.kafka.connect.sink.hana.HANASinkConnector` (kafka-connect-sap 0.9.4): 2,000,000 rows × 3 columns (BIGINT, NVARCHAR(50), DOUBLE).
Schema-embedded JSON via `JsonConverter`. `consumer.override.max.poll.records` set to match `batch.size`. Varying `batch.size` (rows per JDBC `executeBatch`), `tasks.max`.

| batch_size | tasks=1    | tasks=2    | tasks=4    |
|------------|------------|------------|------------|
| 1,000      | 6,645      | 6,579      | 6,601      |
| 5,000      | 20,408     | 20,408     | 20,408     |
| 10,000     | 28,986     | 28,986     | 28,986     |

**Observations:**
- `batch_size` dominates; `tasks.max` has no effect — HANA JDBC insert throughput is single-threaded bounded.
- `batch_size=10,000` peaks at ~29k msg/s (~2M rows in 69s). Larger batches likely continue to help up to HANA JDBC limits.
- `sap_hana` native output (go-hdb `execMany`) is **~2× faster** (~57k vs ~29k) at equivalent batch sizes — single RPC vs JDBC `executeBatch`.
- `consumer.override.max.poll.records` must match `batch.size` — otherwise KC's Kafka consumer caps at 500 records/poll and `batch.size` has no effect.

---

## EC2 — `sap_hana` native

### Bulk Read

`BATCH_COUNT` fixed at 5,000. `TOTAL=5,000,000` rows.

```bash
task bench:load COUNT=5000000
task bench:matrix FETCH="1000 10000 100000 200000 500000" BATCH=5000 CORES="1 2 4 8" OUT=results_v2.txt
```

| fetch_size | cores=1 | cores=2 | cores=4    | cores=8    |
|------------|---------|---------|------------|------------|
| 1,000      | 7,886   | 8,389   | 8,375      | 8,361      |
| 10,000     | 44,248  | 54,348  | 56,180     | 56,180     |
| 100,000    | 41,322  | 73,529  | **83,333** | 61,728     |
| 200,000    | 42,735  | 74,627  | 70,423     | 66,667     |
| 500,000    | 41,322  | 68,493  | 81,967     | **83,333** |

**Best per fetch_size:** 1,000 → 8,389 (cores=2) · 10,000 → 56,180 (cores=4/8) · 100,000 → **83,333** (cores=4) · 200,000 → 74,627 (cores=2) · 500,000 → **83,333** (cores=8)

**Observations:**
- `fetch_size=1000` is the clear bottleneck (~8k, network/round-trip bound) — avoid in production.
- `fetch_size≥100000` plateaus around 41-44k at cores=1, scales to a best of ~81-83k at cores=4 (fetch=100000 or 500000).
- `cores=8` doesn't consistently beat `cores=4` — at fetch=100000/200000 it's actually worse (hyperthread contention, see [Environments](#environments)).
- Pushing fetch size past 100000 gives no further gain.
- **Recommended: `FETCH_SIZE=100000, CORES=4`** (~83k msg/s).

### Incrementing Read

500,000 rows:

```bash
task bench:matrix COUNT=500000 OUT=rpcn_inc.txt
```

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

**Best per fetch_size:** 1,000 → 7,692 (poll=500ms, cores=4/8) · 10,000 → 29,412 (poll=500ms, cores=4) · 100,000 → **33,333** (poll=100ms, cores=2)

**Observations:**
- Same pattern as bulk: `fetch_size=1000` bottlenecks at ~7-8k; `fetch_size≥10000` roughly 2-4×'s it.
- `poll_interval` shows no clean trend — noisy run-to-run (likely HWM-poll timing/network variance). Treat single runs as candidates, not settled numbers.
- Best observed 33,333 msg/s (fetch=100000, cores=2, poll=100ms) — given the noise, treat as a candidate. The 5M re-run below is the more trustworthy number.

500,000 rows wasn't enough to fully separate the top candidates from noise, so this was re-run at 5M scale:

```bash
task bench:matrix COUNT=5000000 FETCH="10000 100000 500000" CORES="2 4 8" POLL="100ms 500ms" OUT=rpcn_inc_v2.txt
```

| fetch_size | poll  | cores=2 | cores=4    | cores=8    |
|------------|-------|---------|------------|------------|
| 10,000     | 100ms | 38,721  | 39,563     | 44,643     |
| 10,000     | 500ms | 37,803  | **47,619** | 34,618     |
| 100,000    | 100ms | 37,803  | 37,689     | 39,643     |
| 100,000    | 500ms | 35,571  | 40,528     | **48,544** |
| 500,000    | 100ms | 36,852  | 39,603     | 40,163     |
| 500,000    | 500ms | 38,566  | 31,955     | 37,519     |

**Best per fetch_size:** 10,000 → 47,619 (poll=500ms, cores=4) · 100,000 → **48,544** (poll=500ms, cores=8) · 500,000 → 40,163 (poll=100ms, cores=8)


### Query Read

500,000 rows (draft):

```bash
task bench:matrix OUT=rpcn_query.txt
```

| fetch_size | cores=1 | cores=2 | cores=4    | cores=8 |
|------------|---------|---------|------------|---------|
| 1,000      | 8,197   | 8,197   | 8,333      | 8,197   |
| 10,000     | 45,455  | 50,000  | 45,455     | 50,000  |
| 100,000    | 55,556  | **71,429** | **71,429** | 62,500  |

Same shape as bulk/incrementing — `fetch_size=1000` bottlenecks at ~8k, jumps to ~45-50k at fetch=10000, best at fetch=100000/cores=2-4 (~71k). 500k rows wasn't enough to separate fetch=100000 from a hypothetical fetch=500000, so this was re-run at 5M scale:

```bash
task bench:load COUNT=5000000
task bench:matrix FETCH="10000 100000 500000" CORES="2 4 8" OUT=rpcn_query_v2.txt
```

| fetch_size | cores=2 | cores=4     | cores=8    |
|------------|---------|-------------|------------|
| 10,000     | 56,180  | 56,180      | 56,818     |
| 100,000    | 94,340  | **100,000** | 96,154     |
| 500,000    | 87,719  | 96,154      | **98,039** |

**Best per fetch_size:** 10,000 → 56,818 (cores=8) · 100,000 → **100,000** (cores=4) · 500,000 → 98,039 (cores=8)

**Observations:**
- All rows hit the full 5,000,000 — clean run.
- `fetch_size=10000` plateaus around ~56k regardless of cores.
- `fetch_size≥100000` jumps to ~88-100k — best single result 100,000 msg/s (fetch=100000, cores=4).
- Core scaling flat past 2-4 at this fetch range.
- **Recommended: `FETCH_SIZE=100000, CORES=4`** (~100k msg/s) — matches the bulk-read recommendation, consistent story across both read modes.

---

## EC2 — `kafka-connect-sap` (comparison)

> Root cause of the throughput gap vs. the other two connectors: see [Architecture note](#architecture-note-why-the-connectors-differ-so-much) above.

### Bulk Read

`BENCH_ORDERS` table, `TOTAL=200000` rows.

```
FETCH       BATCH       TOTAL         ELAPSED     AVG_MSG_S
1000        1000        200000        301s        664
1000        5000        200000        78s         2564
1000        10000       200000        48s         4167
10000       1000        200000        301s        664
10000       5000        200000        77s         2597
10000       10000       200000        48s         4167
100000      1000        200000        298s        671
100000      5000        200000        80s         2500
100000      10000       200000        47s         4255
```

**Observations:**
- `FETCH` (`batch.size`) has essentially no effect — 664/2564/4167 msg/s repeats near-identically across all three `FETCH` values at each `BATCH` level.
- `BATCH` (`batch.max.rows`) drives throughput — 664 → 2564 → 4167 as batch goes 1000 → 5000 → 10000, sub-linearly (fewer, larger LIMIT/OFFSET round trips = less repeated overhead).
- Best: **4,255 msg/s** (fetch=100000, batch=10000) — roughly 20× below the local WSL2 generic-Confluent-JDBC bulk number (86,957 msg/s), and well below `sap_hana` native's EC2 bulk peak (83,333 msg/s).

### Incrementing Read

500,000 rows, `mode=incrementing`.

```
POLL_MS     BATCH         TOTAL         ELAPSED     AVG_MSG_S
100         1000          507000        164s        3091
500         1000          505000        197s        2563
1000        1000          500000        417s        1199
100         5000          500000        197s        2538
500         5000          500000        197s        2538
1000        5000          500000        200s        2500
100         10000         500000        119s        4202
500         10000         500000        116s        4310
1000        10000         500000        120s        4167
```

**Observations:**
- Same shape as bulk: `BATCH` dominates — 1000→~1200-3000 msg/s, 10000→~4200-4300 msg/s.
- `POLL_MS` inconsistent at batch=1000 but converges flat (~4200-4300) at batch=10000.
- Best: **4,310 msg/s** (poll=500ms, batch=10000) — ~10× below the local generic-JDBC incrementing number (41,667 msg/s).
- A couple of rows overshot `TOTAL` slightly (507000, 505000) — loader inserting a few extra rows past target before the connector's count check caught up; not a correctness concern.

**Contention check:** this EC2 box was found running 4 zombie Kafka Connect connectors plus two orphaned native benchmark processes, all competing for HANA connections/CPU during the runs above (idle CPU on the connect container: 24.79% → 2.98% after cleanup). Re-ran the same matrix on a clean box:

```
POLL_MS     BATCH         TOTAL         ELAPSED     AVG_MSG_S
100         1000          500000        765s        654
500         1000          500000        760s        658
1000        1000          500000        763s        655
100         5000          500000        200s        2500
500         5000          500000        196s        2551
1000        5000          500000        200s        2500
100         10000         500000        119s        4202
500         10000         500000        120s        4167
1000        10000         500000        119s        4202
```

`batch=10000` results are essentially identical to the contended run (4202/4167/4202 vs 4202/4310/4167) — **confirms the ~4,200 msg/s ceiling is architectural** (LIMIT/OFFSET per poll + EC2↔HANA network RTT), not resource contention. `batch=1000` came out worse post-cleanup (654-658 vs 1199-3091) — treat as run-to-run noise, not a regression. **~4,200 msg/s at `batch.max.rows=10000` is the confirmed, reproducible number for this connector on this EC2↔HANA path.**

### Query Read

`BENCH_ORDERS` table, raw `query` mode, `TOTAL=200000` rows.

```
FETCH       BATCH       TOTAL         ELAPSED     AVG_MSG_S
1000        1000        200000        301s        664
1000        5000        200000        80s         2500
1000        10000       200000        48s         4167
10000       1000        200000        298s        671
10000       5000        200000        81s         2469
10000       10000       200000        48s         4167
100000      1000        200000        301s        664
100000      5000        200000        77s         2597
100000      10000       200000        47s         4255
```

**Observations:**
- Same shape and near-identical numbers to this connector's bulk-read results — query mode hits the same LIMIT/OFFSET-per-poll bottleneck; `kafka-connect-sap` has no distinct held-cursor path for raw queries either.
- Best: **4,255 msg/s** (fetch=100000, batch=10000) — ~10× below the generic Confluent JDBC connector's query-mode peak on the same EC2↔HANA path.

---

## EC2 — Generic Confluent JDBC (comparison)

`io.confluent.connect.jdbc.JdbcSourceConnector` (via confluent-hub) + `ngdbc.jar`, `mode=bulk`, raw `query` (not `table.whitelist`), `fetchsize` passed as a JDBC URL param. Separate container/port (8084) from the `kafka-connect-sap` harness above, run independently — not concurrently.

### Bulk Read

`BENCH_ORDERS` table, `TOTAL=100000` rows:

```
FETCH       BATCH       TOTAL         ELAPSED     AVG_MSG_S
1000        1000        100000        27s         3704
1000        5000        100000        9s          11111
1000        10000       100000        6s          16667
10000       1000        100000        27s         3704
10000       5000        100000        9s          11111
10000       10000       100000        6s          16667
100000      1000        100000        27s         3704
100000      5000        100000        9s          11111
100000      10000       100000        6s          16667
```

Reloaded to `TOTAL=2000000` rows:

```
FETCH       BATCH       TOTAL         ELAPSED     AVG_MSG_S
1000        1000        2000000       458s        4367
1000        5000        2000000       99s         20202
1000        10000       2000000       54s         37037
10000       1000        2000000       457s        4376
10000       5000        2000000       95s         21053
10000       10000       2000000       51s         39216
100000      1000        2000000       402          4973
100000      5000        2000000       80s         25000
100000      10000       2000000       48s         41667
```


**Observations:**
- Same shape as every matrix in this doc: `FETCH` has essentially no effect, `BATCH` drives throughput — 4,367 → 20,202 → 37,037 as batch goes 1000 → 5000 → 10000.
- Peak **41,667 msg/s** (fetch=100000, batch=10000) — roughly 10× above `kafka-connect-sap`'s confirmed ~4,200 msg/s ceiling on this same EC2↔HANA path. Confirms the held-cursor advantage holds on EC2, not just locally.
- Still below the local WSL2 figure for this connector (86,957 msg/s) — consistent with EC2↔HANA network RTT (~118ms vs ~70ms local) adding overhead per round trip.

### Incrementing Read

`BENCH_ORDERS` table, `mode=incrementing`, `TOTAL=2000000` rows:

```
POLL_MS     BATCH         TOTAL         ELAPSED     AVG_MSG_S
100         1000          2000000       459s        4357
500         1000          2000000       459s        4357
1000        1000          ERROR         ?           0
100         5000          2000000       168s        11905
500         5000          1995000       125s        15960
1000        5000          2000000       104s        19231
100         10000         2000000       57s         35088
500         10000         2000000       57s         35088
1000        10000         1995000       83s         24036
```

**Observations:**
- Same shape as bulk: `BATCH` dominates — 4,357 at batch=1000 → ~15-19k at batch=5000 → ~24-35k at batch=10000. `POLL_MS` negligible/inconsistent.
- `poll=1000, batch=1000` errored (not yet re-run in isolation).
- Peak **35,088 msg/s** (poll=100 or 500, batch=10000) — well above `kafka-connect-sap`'s incrementing ceiling (~4,200 msg/s), consistent with the bulk-read advantage.

### Query Read

`BENCH_ORDERS` table, raw `query` mode, `TOTAL=2000000` rows:

```
FETCH       BATCH       TOTAL         ELAPSED     AVG_MSG_S
1000        1000        2000000       453s        4415
1000        5000        2000000       96s         20833
1000        10000       2000000       51s         39216
10000       1000        2000000       453s        4415
10000       5000        2000000       99s         20202
10000       10000       2000000       50s         40000
100000      1000        2000000       459s        4357
100000      5000        2000000       96s         20833
100000      10000       2000000       51s         39216
```

**Observations:**
- Same shape as bulk and incrementing: `FETCH` negligible, `BATCH` drives throughput — ~4,400 at batch=1000 → ~20-21k at batch=5000 → ~39-40k at batch=10000.
- Peak **40,000 msg/s** (fetch=10000, batch=10000) — in line with this connector's bulk (41,667) and incrementing (35,088) peaks, confirming the held-cursor advantage holds across all three read modes.

---

