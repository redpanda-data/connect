# kafka-source — Debezium MySQL Source Benchmark

Measures throughput of the **Debezium MySQL source connector** reading from MySQL into a Kafka topic.
Also includes a **Redpanda Connect CDC** benchmark reusing the same MySQL and Kafka containers.

Used as a comparison baseline against Redpanda Connect's `mysql_cdc` input.

See [`docs/benchmark-results/mysql-cdc.md`](../../../../../../docs/benchmark-results/mysql-cdc.md) for full results.

---

## Prerequisites

Docker must be running.

## Start

```bash
task up   # builds Debezium image, starts MySQL + Kafka + Kafka Connect — waits until ready
```

## Debezium — Snapshot benchmark

### Load data

```bash
task bench:load COUNT=10000000   # creates cart table and inserts 10M rows
```

### Run benchmark

```bash
task bench:run FETCH_SIZE=5000 BATCH_SIZE=5000   # registers connector and measures snapshot throughput
```

### Run all combinations

```bash
task bench:load COUNT=10000000
task bench:all OUT=results.txt   # runs fetch/batch = 1000, 5000, 10000
```

## Debezium — CDC benchmark

### Prepare (no data loaded)

```bash
task bench:load:cdc   # truncates cart table and prepares insert procedure
```

### Run benchmark

```bash
task bench:run:cdc COUNT=1000000 BATCH_SIZE=5000   # starts connector, inserts rows, measures throughput
```

### Run all combinations

```bash
task bench:load:cdc
task bench:all:cdc COUNT=1000000 OUT=results.txt   # runs batch = 1000, 5000, 10000
```

## Redpanda Connect — CDC benchmark

### Build binary

```bash
task rpcn:build   # builds binary to /tmp/myks-rpcn-bench
```

### Prepare (no data loaded)

```bash
task bench:load:cdc   # same setup as Debezium CDC
```

### Run benchmark

```bash
task bench:run:rpcn:cdc COUNT=1000000 BATCH_SIZE=5000
```

### Run all combinations

```bash
task bench:load:cdc
task bench:all:rpcn:cdc COUNT=1000000 OUT=results.txt   # runs batch = 1000, 5000, 10000
```

### View logs

```bash
task rpcn:logs   # tails /tmp/myks_rpcn_bench_cdc.log
```

## Stop

```bash
task down   # stops and removes all containers and volumes
```
