# Benchmarking Iceberg Output Component

Benchmark demonstrating write throughput of Redpanda's Iceberg output connector.

Unlike the PostgreSQL CDC benchmark (which measures read throughput), this benchmark measures **write throughput** — how fast the connector can write generated messages to Iceberg tables backed by MinIO (S3-compatible) and an Apache Iceberg REST catalog.

## Prerequisites

- Docker with Compose

## Setup

Start MinIO and the Iceberg REST catalog:

```bash
task infra:up
```

This starts:
- **MinIO** on port 9000 (S3-compatible storage), console at http://localhost:9001 (admin/password)
- **Iceberg REST catalog** on port 8181

## Benchmark Tasks

### Parameterised benchmark

```bash
task bench CORES=4 BATCH=5000 COUNT=1000000
```

- `CORES` sets `GOMAXPROCS` (omit for unbounded)
- `BATCH` sets `batching.count` (defaults to 1000)
- `COUNT` sets number of messages to generate (defaults to 0 = unlimited, run until CTRL+C)

### max_in_flight benchmark

Fixed at `CORES=4`, varying `BATCH` and `MIF` (max_in_flight):

```bash
task bench:mif BATCH=10000 MIF=32 COUNT=1000000
```

- `BATCH` sets `batching.count` (defaults to 1000)
- `MIF` sets `max_in_flight` (defaults to 4)
- `CORES` sets `GOMAXPROCS` (defaults to 4)
- `COUNT` sets number of messages to generate (defaults to 1000000)

### Reset and run

Wipes all data, restarts infrastructure, then runs the benchmark:

```bash
task bench:reset CORES=4 BATCH=5000 COUNT=1000000
```

Use this between runs to ensure a clean state.

## Data Management

```bash
task infra:reset   # wipe all data and restart infrastructure
task infra:down    # stop and remove all containers and volumes
task infra:logs    # follow container logs
```

## Expected Output

```
INFO rolling stats: 5000 msg/sec, 3.2 MB/sec    @service=redpanda-connect ...
INFO rolling stats: 5000 msg/sec, 3.1 MB/sec    @service=redpanda-connect ...
```

The benchmark processor is on the pipeline (before the output), so it measures the rate at which messages enter the Iceberg writer, including any batching overhead.

See [`docs/benchmark-results/iceberg.md`](../../../../docs/benchmark-results/iceberg.md) for full results.
