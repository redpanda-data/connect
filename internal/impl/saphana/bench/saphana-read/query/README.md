# SAP HANA Query Read Benchmark

Measures `sap_hana` input throughput in `query` mode: executes a user-supplied SQL statement and produces one message per result row. Pipeline terminates after all result rows are emitted.

## What it measures

Throughput of arbitrary SQL — full scans, filtered queries, or queries with computed columns — without the overhead of table-level metadata. Useful for comparing selective vs full-scan performance.

## Prerequisites

- Docker running (Kafka — run `task up` from `saphana-read/bulk/` if not already started)
- `HANA_DSN` and `HANA_SCHEMA` env vars set

## Setup

```bash
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="YOUR_SCHEMA"

task bench:build                  # build redpanda-connect + loader binaries
task bench:load COUNT=1000000     # drop/create BENCH_ORDERS_QUERY, insert 1M rows
```

## Run

### Full scan (default)

```bash
task bench:run                    # runs: SELECT * FROM "SCHEMA"."BENCH_ORDERS_QUERY"
task bench:run CORES=4
```

### Filtered query

```bash
export HANA_QUERY='SELECT * FROM "DAVIDDEDU"."BENCH_ORDERS_QUERY" WHERE STATUS = '"'"'confirmed'"'"''
task bench:run
```

### Aggregation / computed columns

```bash
export HANA_QUERY='SELECT USER_ID, SUM(PRICE) AS TOTAL FROM "DAVIDDEDU"."BENCH_ORDERS_QUERY" GROUP BY USER_ID'
task bench:run
```

Output:
```
cores=4
query=SELECT * FROM "DAVIDDEDU"."BENCH_ORDERS_QUERY"
TIME        IN KAFKA        MSG/S
10:00:01    14200           7100 msg/s
...
---
Total messages : 1000000
Elapsed        : 108s
Avg throughput : 9259 msg/s
```

## Run all core combinations

```bash
task bench:all OUT=results.txt    # sweeps CORES=1,2,4,8
```

## Teardown

Stop Kafka from the bulk bench directory:

```bash
cd ../bulk && task down
```
