# SAP HANA Query Read Benchmark

Measures throughput of arbitrary SQL via two connectors:

- **`sap_hana` input** (`query` mode) — executes a user-supplied SQL statement via go-hdb native protocol
- **Kafka Connect JDBC Source** (`bulk` mode with custom query) — comparison baseline using ngdbc JDBC driver

## What it measures

Throughput of arbitrary SQL — full scans, filtered queries, or queries with computed columns — without table-level metadata overhead.

## Prerequisites

- Docker running
- `HANA_DSN` and `HANA_SCHEMA` env vars set
- For Kafka Connect benchmarks: `bench/ngdbc.jar` present (SAP HANA JDBC driver)

## Setup

```bash
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="YOUR_SCHEMA"

task up                           # start Kafka
task bench:build                  # build redpanda-connect + loader binaries
task bench:load COUNT=2000000     # drop/create BENCH_ORDERS_QUERY, insert 2M rows
```

---

## Redpanda Connect — sap_hana query mode

### Single run

```bash
task bench:run                    # SELECT * FROM "SCHEMA"."BENCH_ORDERS_QUERY"
task bench:run CORES=4 FETCH_SIZE=100000
```

### Custom query

```bash
export HANA_QUERY='SELECT ID, USER_ID, CAST(PRICE AS DOUBLE) AS PRICE FROM "SCHEMA"."BENCH_ORDERS_QUERY" WHERE STATUS = '"'"'confirmed'"'"''
task bench:run CORES=4
```

### Full matrix (fetch_size × cores)

```bash
task bench:matrix OUT=results.txt
```

Parameters: `FETCH_SIZE` (default 10000), `CORES` (default unbounded), `BATCH_COUNT` (default 1000), `MAX_IN_FLIGHT` (default 10).

---

## Kafka Connect JDBC Source (comparison)

### Prerequisites

```bash
# Build the Kafka Connect image (one-time, requires internet)
task kc:build

# Start Kafka Connect (Kafka must already be running via task up)
task kc:up
```

### Single run

```bash
task bench:kc:run FETCH_SIZE=10000 BATCH_MAX_ROWS=10000 TOTAL=2000000
```

### Full matrix (fetch_size × batch_max_rows)

```bash
task bench:kc:matrix TOTAL=2000000 OUT=kc_query.txt
```

Parameters: `FETCH_SIZE` (default 10000), `BATCH_MAX_ROWS` (default 10000), `TOTAL` (default 2000000).

The connector uses `mode=bulk` with `poll.interval.ms=86400000` (one-shot read).
`PRICE` is cast to `DOUBLE` to avoid an ngdbc 2.x hang with `DECIMAL+NVARCHAR+TIMESTAMP` result sets.

---

## Teardown

```bash
task down
```
