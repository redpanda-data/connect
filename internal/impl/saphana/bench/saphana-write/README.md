# SAP HANA Write Benchmark

Measures write throughput into SAP HANA via two connectors:

- **`sap_hana` output** — Kafka → HANA using go-hdb native protocol (`execMany` bulk insert)
- **Kafka Connect JDBC Sink** — Kafka → HANA via ngdbc JDBC driver (comparison)

## What it measures

End-to-end write throughput from Kafka consumer to HANA INSERT, across different batch sizes and parallelism settings.

## Prerequisites

- Docker running
- `HANA_DSN` and `HANA_SCHEMA` env vars set
- For Kafka Connect benchmarks: `bench/ngdbc.jar` present (SAP HANA JDBC driver)

## Setup

```bash
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="YOUR_SCHEMA"

task up                           # start Kafka
task bench:build                  # build redpanda-connect, count, and setup binaries
task bench:setup                  # create empty BENCH_WRITES table in HANA
task bench:load COUNT=2000000     # produce 2M messages to Kafka topic hana-bench-writes
```

---

## Redpanda Connect — sap_hana output

### Single run

```bash
task bench:run                           # unbounded cores, max_in_flight=10
task bench:run CORES=8 MAX_IN_FLIGHT=10
```

### Full matrix (max_in_flight × cores)

```bash
task bench:matrix OUT=results.txt
```

Parameters: `MAX_IN_FLIGHT` (default 10), `CORES` (default unbounded).

---

## Kafka Connect JDBC Sink (comparison)

Uses `io.confluent.connect.jdbc.JdbcSinkConnector` with `insert.mode=insert`.
Messages are schema-enabled JSON (ID BIGINT, CATEGORY NVARCHAR(50), VALUE DOUBLE).
Written to the same `BENCH_WRITES` table; TS column receives NULL (not included in schema).

### Prerequisites

```bash
# Build the Kafka Connect image (one-time, requires internet)
task kc:build

# Start Kafka Connect (Kafka must already be running via task up)
task kc:up

# Produce schema-enabled messages to KC topic (separate from RPCN topic)
task bench:kc:load COUNT=2000000
```

### Single run

```bash
task bench:kc:run BATCH_SIZE=1000 TASKS_MAX=1 TOTAL=2000000
```

### Full matrix (batch_size × tasks_max)

```bash
task bench:kc:matrix TOTAL=2000000 OUT=kc_write.txt
```

Parameters: `BATCH_SIZE` (default 1000), `TASKS_MAX` (default 1), `TOTAL` (default 2000000).

`BATCH_SIZE` controls rows per INSERT call (analogous to RPCN `batching.count`).
`TASKS_MAX` controls parallel Kafka consumer/writer tasks (analogous to RPCN `max_in_flight`).

---

## Teardown

```bash
task down
```
