# rpcn-source — Redpanda Connect MySQL → Kafka Benchmark

Measures throughput of **Redpanda Connect** reading from MySQL (`mysql_cdc` input) and writing to a Kafka topic (`kafka_franz` output).
Covers two modes: full **snapshot** read and **CDC** (change event streaming).

See [`docs/benchmark-results/mysql-cdc.md`](../../../../../../docs/benchmark-results/mysql-cdc.md) for full results.

---

## Prerequisites

Docker must be running.

## Start

```bash
task up   # starts MySQL and Kafka — waits until ready
```

## Build binary

```bash
task bench:build   # builds binary to /tmp/rpcns-bench
```

## Snapshot benchmark

### Load data

```bash
task bench:load COUNT=10000000   # creates cart table and inserts 10M rows
```

### Run benchmark

```bash
task bench:run CORES=4 BATCH=1000   # reads full snapshot and measures throughput
```

`CORES` sets `GOMAXPROCS` (omit for unbounded). `BATCH` sets `batching.count` (default: 1000).

### Run all combinations

```bash
task bench:load COUNT=10000000
task bench:all OUT=results.txt   # sweeps cores = 1, 2, 4, 8 × batch = 1000, 5000, 10000
```

## CDC benchmark

### Prepare (no data loaded)

```bash
task bench:load:cdc   # truncates cart table and prepares insert procedure
```

### Run benchmark

```bash
task bench:run:cdc CORES=1 BATCH=5000 COUNT=10000000
```

### Run all combinations

```bash
task bench:load:cdc
task bench:all:cdc COUNT=10000000 OUT=results.txt   # sweeps cores = 1, 2, 4, 8 × batch = 1000, 5000, 10000
```

## View logs

```bash
task logs       # tails snapshot run log (/tmp/rpcns_bench.log)
task logs:cdc   # tails CDC run log (/tmp/rpcns_bench_cdc.log)
```

## Stop

```bash
task down   # stops and removes all containers and volumes
```
