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
