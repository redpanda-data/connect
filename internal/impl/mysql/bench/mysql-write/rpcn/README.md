# redpanda-connector — Redpanda Connect Kafka → MySQL Benchmark

Measures throughput of **Redpanda Connect** consuming from a Kafka topic (`kafka_franz` input) and writing to MySQL (`sql_insert` output).
Used as a comparison baseline against the Confluent JDBC Sink connector.

See [`docs/benchmark-results/mysql-cdc.md`](../../../../../../docs/benchmark-results/mysql-cdc.md) for full results.

---

## Prerequisites

Docker must be running.

## Start

```bash
task up   # starts Kafka and MySQL — waits until ready
```

## Load data

```bash
task bench:load COUNT=10000000   # produces 10M events to the bench-events Kafka topic
```

## Run benchmark

```bash
task bench:run CORES=4 BATCH=10000   # consumes from Kafka and measures MySQL write throughput
```

`CORES` sets `GOMAXPROCS` (default: 1). `BATCH` sets the output batch size (default: 10000).

## Run all combinations

```bash
task bench:load COUNT=10000000
task bench:run CORES=4 BATCH=10000
task bench:run CORES=8 BATCH=10000
```

## Stop

```bash
task down   # stops and removes all containers and volumes
```
