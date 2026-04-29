# kafka-connector — Kafka Connect JDBC Sink Benchmark

Measures throughput of the **Confluent JDBC Sink connector** writing from a Kafka topic into MySQL.
Used as a comparison baseline against Redpanda Connect's `sql_insert` output.

See [`docs/benchmark-results/mysql-cdc.md`](../../../../../../docs/benchmark-results/mysql-cdc.md) for full results.

---

## Prerequisites

Docker must be running.

## Start

```bash
task up   # starts Kafka, MySQL, and Kafka Connect with JDBC plugin — waits until ready
```

## Load data

```bash
task bench:load COUNT=10000000   # produces 10M events to the bench-events Kafka topic
```

## Run benchmark

```bash
task bench:run TASKS=16   # registers the JDBC sink connector and measures write throughput
```

`TASKS` sets `tasks.max` on the connector (default: 16).

## Run all combinations

```bash
task bench:load COUNT=10000000
task bench:run TASKS=4
task bench:run TASKS=8
task bench:run TASKS=16
```

## Stop

```bash
task down   # stops and removes all containers and volumes
```
