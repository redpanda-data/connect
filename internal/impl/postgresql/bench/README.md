# Benchmarking PostgreSQL CDC Component

Benchmark demonstrating throughput of Redpanda's PostgreSQL CDC Connector.

## Prerequisites

Install `psql`:

```bash
# macOS
brew install postgresql

# Ubuntu/Debian
sudo apt install postgresql-client
```

## Setup

1. Start PostgreSQL container:

```bash
task postgres:up
```

This starts a PostgreSQL 16 container with `wal_level=logical` enabled (required for CDC).

2. Create tables:

```bash
task psql:create
```

3. Insert test data:

```bash
task psql:data:users     # 150K rows, ~625KB per row (large payload)
task psql:data:products  # 150K rows, ~625KB per row (large payload)
task psql:data:cart      # 10M rows, small payloads
```

## Benchmark Tasks

### Parameterised benchmarks

Truncate, load the dataset, and run with configurable core count and batch size:

```bash
task bench:users CORES=2 BATCH=1000   # 150K large rows (~625 KB each) — I/O bound
task bench:cart  CORES=4 BATCH=5000   # 10M small rows — CPU bound
```

- `CORES` sets `GOMAXPROCS` (omit for unbounded)
- `BATCH` sets `batching.count` (defaults to 1000)

### Snapshot benchmarks

```bash
task bench:run                # run with current table data
task bench:snapshot:users     # truncate + insert 150K large rows + run
task bench:snapshot:cart      # truncate + insert 10M small rows + run
task bench:snapshot:all       # truncate + insert all datasets + run
```

### CPU scaling (fixed batch=1000)

```bash
task bench:cores:1
task bench:cores:2
task bench:cores:4
task bench:cores:8
```

### CDC mode

Start the connector first (it creates the replication slot), then insert data in a second terminal while it is running:

```bash
# terminal 1
task bench:run:cdc

# terminal 2 — once the connector is up
task psql:data:users
```

## Data Management

```bash
task psql:truncate    # truncate all tables
task psql:drop-slot   # drop bench_slot so the next run replays from the start
```

Drop the replication slot before every benchmark run. Unused slots cause PostgreSQL to retain WAL segments indefinitely, which can fill up disk.

## Expected Output

```
INFO rolling stats: 1000 msg/sec, 625 MB/sec    @service=redpanda-connect bytes/sec=6.2527229e+08 label="" msg/sec=1000 path=root.output.processors.0
INFO rolling stats: 2000 msg/sec, 1.3 GB/sec    @service=redpanda-connect bytes/sec=1.25054458e+09 label="" msg/sec=2000 path=root.output.processors.0
```

Throughput with large rows (~625 KB each) is I/O bound — CPU core count has little effect. Use the cart dataset to stress CPU. See [`docs/benchmark-results/postgres.md`](../../../../docs/benchmark-results/postgres.md) for full results.
