# SAP HANA Write Benchmark

Measures `sql_insert` (driver: hana) write throughput: consumes pre-filled Kafka messages and appends them to a SAP HANA table using parameterized INSERT statements.

## What it measures

End-to-end write throughput from Kafka consumer to HANA INSERT, across different batch sizes and core counts.

## Prerequisites

- Docker running (Kafka — run `task up` from `saphana-read/bulk/` if not already started)
- `HANA_DSN` and `HANA_SCHEMA` env vars set

## Setup

```bash
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="YOUR_SCHEMA"

task bench:build                  # build binaries
task bench:load COUNT=1000000     # create topic + produce 1M messages to Kafka
```

## Run

```bash
task bench:run                           # unbounded cores, max_in_flight=10
task bench:run CORES=4 MAX_IN_FLIGHT=20
```

Output:
```
cores=4  max_in_flight=20
TIME        WRITTEN         MSG/S
10:00:02    15000           7500 msg/s
10:00:04    36000           10500 msg/s
...
---
Total messages : 1000000 / 1000000
Elapsed        : 89s
Avg throughput : 11235 msg/s
```

## Run all combinations

```bash
task bench:all OUT=results.txt    # sweeps CORES=1,2,4,8 × MAX_IN_FLIGHT=5,10,20,50
```

## Teardown

Stop Kafka from the bulk bench directory:

```bash
cd ../saphana-read/bulk && task down
```
