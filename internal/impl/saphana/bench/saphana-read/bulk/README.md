# SAP HANA Bulk Read Benchmark

Measures `sap_hana` input throughput in `bulk` mode: reads all rows from a table once and produces one message per row to Kafka.

## What it measures

Pipeline terminates after all rows are emitted. Elapsed time and average msg/s are captured from start to last Kafka offset.

## Prerequisites

- Docker running
- `HANA_DSN` and `HANA_SCHEMA` env vars set

## Setup

```bash
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="YOUR_SCHEMA"

task up                           # start Kafka
task bench:build                  # build redpanda-connect + loader binaries
task bench:load COUNT=1000000     # drop/create BENCH_ORDERS, insert 1M rows
```

## Run

```bash
task bench:run                    # unbounded cores
task bench:run CORES=4            # pin GOMAXPROCS
```

Output:
```
cores=4
TIME        IN KAFKA        MSG/S
10:00:01    12543           6271 msg/s
10:00:03    31204           9330 msg/s
...
---
cores=4
Total messages : 1000000
Elapsed        : 112s
Avg throughput : 8928 msg/s
```

## Run all combinations

```bash
task bench:all OUT=results.txt    # sweeps CORES=1,2,4,8
```

## Teardown

```bash
task down
```
