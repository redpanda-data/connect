# SAP HANA Incrementing Read Benchmark

Measures `sap_hana` input throughput in `incrementing` mode: connector stays running and polls for net-new rows using `ID` as a high-water mark column. Rows inserted while the connector runs are picked up on the next poll.

## What it measures

Connector starts against an empty table, rows are inserted concurrently, elapsed time and avg msg/s are measured from first insert to last Kafka offset.

## Prerequisites

- Docker running (shares Kafka with bulk bench — run `task up` from `saphana-read/bulk/` if not already started)
- `HANA_DSN` and `HANA_SCHEMA` env vars set

## Setup

```bash
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="YOUR_SCHEMA"

task bench:build                  # build redpanda-connect + loader binaries
```

## Run

```bash
task bench:run COUNT=100000       # unbounded cores, 1s poll interval
task bench:run COUNT=100000 CORES=4 POLL=500ms
```

Output:
```
cores=4  count=100000  poll=500ms
TIME        IN KAFKA        MSG/S
10:00:04    5000            2500 msg/s
10:00:06    14200           4600 msg/s
...
---
Total messages : 100000 / 100000
Elapsed        : 38s
Avg throughput : 2631 msg/s
```

## Run all combinations

```bash
task bench:all COUNT=100000 OUT=results.txt   # sweeps CORES=1,2,4,8 × POLL=100ms,500ms,1s
```

## Tests

### bench:test:skip-existing

Verifies the connector skips pre-existing rows and only emits net-new rows after startup.

```bash
task bench:test:skip-existing
```

See root `README.md` for full description.

## Teardown

Stop the connector (auto-killed by bench tasks). To stop Kafka:

```bash
cd ../bulk && task down
```
