# Benchmarking Oracle CDC Component

Benchmark demonstrating throughput of Redpanda's Oracle CDC Connector, with an optional Debezium comparison.

## Prerequisites

- Docker
- [sqlcl](https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/) (`brew install oracle-instantclient sqlcl`)
- An Oracle container registry account — accept the terms at https://container-registry.oracle.com before pulling

## Redpanda Connect Benchmark

### 1. Start Oracle

```bash
task oracledb:up
```

Wait for the database to be ready (check with `task oracledb:logs` — look for `DATABASE IS READY TO USE!`).

### 2. Enable ARCHIVELOG mode (required for LogMiner)

```bash
task oracledb:archivelog
task rman:setup
```

### 3. Create test tables

```bash
task sqlcl:create
```

### 4. Start Redpanda Connect

```bash
go run ../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

### 5. Generate test data

In a separate terminal, run one or more of the following:

```bash
task sqlcl:data:users      # inserts rows into TESTDB.USERS
task sqlcl:data:products   # inserts 500k rows into TESTDB.PRODUCTS
task sqlcl:data:cart       # inserts rows into TESTDB.CART
```

Redpanda Connect will stream the CDC events via LogMiner as data is inserted.

### 6. Clear checkpoint cache between runs

```bash
task sqlcl:drop-cache
```

### Expected Output

```
INFO rolling stats: 91733 msg/sec, 123 MB/sec      @service=redpanda-connect bytes/sec=1.22793538e+08 label="" msg/sec=91733 path=root.output.processors.0
INFO rolling stats: 101267 msg/sec, 136 MB/sec     @service=redpanda-connect bytes/sec=1.35555936e+08 label="" msg/sec=101267 path=root.output.processors.0
```
