# SAP HANA Benchmark Results

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.114.1), x86_64

See [`internal/impl/saphana/bench/`](../../internal/impl/saphana/bench/) for configs and run instructions.

---

## Bulk Read

Full scan of `BENCH_ORDERS`: 2,000,000 rows × ~300 B (BIGINT, INTEGER × 3, DECIMAL, NVARCHAR(20), NVARCHAR(200), TIMESTAMP).
Pipeline: `sap_hana` input (bulk mode) → `kafka_franz` output. `max_in_flight=10`.
Varying `fetch_size`, `batching.count`, and `GOMAXPROCS`.

### msg/sec

| fetch_size | batch=1000 | batch=5000 | batch=10000 |
|------------|------------|------------|-------------|
| 1,000      | 20,202 – 21,978 | 19,231 – 21,739 | 18,868 – 19,802 |
| 10,000     | 36,364 – 40,816 | 37,736 – 44,444 | 32,258 – **48,780** |
| 100,000    | 32,258 – 45,455 | 38,462 – 43,478 | 37,736 – 46,512 |

Ranges show min–max across `GOMAXPROCS` 1, 2, 4, 8.

### Best result per fetch_size (across all batch and core counts)

| fetch_size | Best msg/s | Config |
|------------|------------|--------|
| 1,000      | 21,978 | batch=1000, cores=8 |
| 10,000     | 48,780 | batch=10000, cores=8 |
| 100,000    | 46,512 | batch=10000, cores=4 |

**Observations:**
- `fetch_size` is the dominant parameter. Increasing from 1,000 to 10,000 roughly doubles throughput by reducing HANA `FetchNext` round-trips for 2M rows from ~2,000 to ~200.
- Increasing `fetch_size` from 10,000 to 100,000 yields marginal gains; the bottleneck shifts from HANA network to Kafka produce and Go processing.
- Kafka `batching.count` has secondary effect. Larger batches reduce Kafka produce round-trips but do not compensate for a small `fetch_size`.
- Core scaling is weak because the pipeline is dominated by sequential HANA cursor reads (single connection, single result set). Extra goroutines help overlap Kafka I/O with HANA processing but saturate quickly.
- **Recommended configuration: `fetch_size=10000`, `batching.count=10000`, `GOMAXPROCS=8` → ~49,000 msg/s (~2M rows in 41s).**

---

## Incrementing Read

Concurrent load + capture: 500,000 rows inserted via 10 parallel workers while the connector polls for new rows.
Pipeline: `sap_hana` input (incrementing mode, `incrementing_column=ID`) → `kafka_franz` output. `max_in_flight=10`, `batching.count=1000`.
Varying `fetch_size`, `GOMAXPROCS`, and `poll_interval`.

### msg/sec

| fetch_size | poll | cores=1 | cores=2 | cores=4 | cores=8 |
|------------|------|---------|---------|---------|---------|
| 1,000      | 100ms | 20,000 | 20,000 | 21,739 | 22,727 |
| 1,000      | 500ms | 20,000 | 20,000 | 17,857 | 19,231 |
| 1,000      | 1s    | 20,000 | 20,000 | 20,000 | 19,231 |
| 10,000     | 100ms | 31,250 | 31,250 | 38,462 | 38,462 |
| 10,000     | 500ms | 31,250 | 38,462 | 38,462 | 41,667 |
| 10,000     | 1s    | 26,316 | 31,250 | 31,250 | 38,462 |
| 100,000    | 100ms | 31,250 | 38,462 | 38,462 | 22,727 |
| 100,000    | 500ms | 26,316 | 38,462 | 31,250 | 38,462 |
| 100,000    | 1s    | 15,625 | 20,000 | 38,462 | 38,462 |

### Best result per fetch_size (across all poll and core counts)

| fetch_size | Best msg/s | Config |
|------------|------------|--------|
| 1,000      | 22,727 | poll=100ms, cores=8 |
| 10,000     | **41,667** | poll=500ms, cores=8 |
| 100,000    | 38,462 | poll=100ms/500ms, cores=2/4/8 |

**Observations:**
- `fetch_size` is again the dominant parameter. Increasing from 1,000 to 10,000 roughly doubles throughput (~22k → ~42k msg/s).
- `fetch_size=100000` does not improve over 10,000 and is slightly slower in some configurations due to larger result-set transfer per poll.
- `poll_interval=100ms` performs best at `fetch_size=1000` where more frequent polls compensate for small batches. At larger fetch sizes `poll_interval` has less effect since each poll already returns a large batch.
- Core scaling is modest; the bottleneck is HANA cursor read latency, not CPU.
- **Recommended configuration: `fetch_size=10000`, `poll_interval=100ms–500ms`, `GOMAXPROCS=4–8` → ~38,000–42,000 msg/s.**

---

## Query Read

Full scan via user-supplied SQL: 2,000,000 rows × ~300 B (BIGINT, INTEGER × 3, DECIMAL, NVARCHAR(20), NVARCHAR(200), TIMESTAMP).
Pipeline: `sap_hana` input (query mode) → `kafka_franz` output. `max_in_flight=10`.
Query: `SELECT * FROM "SCHEMA"."BENCH_ORDERS_QUERY"`. Varying `fetch_size` and `GOMAXPROCS`.

### msg/sec

| fetch_size | cores=1 | cores=2 | cores=4 | cores=8 |
|------------|---------|---------|---------|---------|
| 1,000      | 22,727  | 22,222  | 21,505  | 23,529  |
| 10,000     | 68,966  | 76,923  | 76,923  | 71,429  |
| 100,000    | 62,500  | 68,966  | **95,238** | 90,909 |

### Best result per fetch_size (across all core counts)

| fetch_size | Best msg/s | Config |
|------------|------------|--------|
| 1,000      | 23,529 | cores=8 |
| 10,000     | 76,923 | cores=2 or 4 |
| 100,000    | **95,238** | cores=4 |

**Observations:**
- `fetch_size` is again the dominant parameter. Increasing from 1,000 to 10,000 roughly triples throughput (~23k → ~77k msg/s).
- Unlike bulk mode, `fetch_size=100,000` outperforms 10,000 at higher core counts (~95k vs ~77k msg/s). Query mode does not iterate a server-side cursor between fetches; a larger fetch size directly reduces HANA round-trips per result set.
- Core scaling is more effective at `fetch_size=100,000`: cores=4 achieves the overall peak, suggesting that larger result transfers benefit from more parallel Kafka produce capacity.
- **Recommended configuration: `fetch_size=100000`, `GOMAXPROCS=4` → ~95,000 msg/s (~2M rows in 21s).**

---

## Write

Kafka → `sap_hana` output (native bulk insert): 2,000,000 rows × 4 columns (BIGINT, NVARCHAR(50), DOUBLE, TIMESTAMP).
Pipeline: `kafka_franz` input → `sap_hana` output. Each batch is a single `MtInsert` RPC via go-hdb execMany.
`batching.count=1000`. Varying `max_in_flight` (concurrent batch INSERT calls) and `GOMAXPROCS`.

### msg/sec

| max_in_flight | cores=1 | cores=2 | cores=4 | cores=8 |
|---------------|---------|---------|---------|---------|
| 5             | 31,250  | 34,483  | 36,364  | 36,364  |
| 10            | 28,986  | 51,282  | 40,816  | **57,143** |
| 20            | 28,169  | 33,333  | 35,714  | 37,736  |
| 50            | 15,873  | 18,868  | 22,989  | 19,231  |

### Best result per max_in_flight (across all core counts)

| max_in_flight | Best msg/s | Config |
|---------------|------------|--------|
| 5             | 36,364 | cores=4 or 8 |
| 10            | **57,143** | cores=8 |
| 20            | 37,736 | cores=8 |
| 50            | 22,989 | cores=4 |

**Observations:**
- `max_in_flight=10` is the sweet spot: ~57k msg/s at 8 cores. Lower values under-saturate HANA; higher values cause contention.
- `max_in_flight=50` degrades significantly (~23k msg/s peak) — too many concurrent INSERT RPCs overwhelm HANA's concurrency handling.
- Core scaling is most effective at `max_in_flight=10`: throughput nearly doubles from cores=1 to cores=8 (~29k → ~57k msg/s).
- At `max_in_flight=5` and `max_in_flight=20`, gains plateau above cores=4, suggesting the bottleneck shifts to HANA INSERT latency.
- **Recommended configuration: `max_in_flight=10`, `batching.count=1000`, `GOMAXPROCS=8` → ~57,000 msg/s (~2M rows in 35s).**
