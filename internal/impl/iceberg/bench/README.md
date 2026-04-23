# Iceberg Benchmark

Measures write throughput of the Redpanda Connect Iceberg output. Two benchmarks are available:

- **This folder** — Redpanda Connect only (generate → Iceberg), no Kafka
- **[`kafka-connector/`](kafka-connector/)** — End-to-end comparison: Kafka → transform → Iceberg, benchmarked against Kafka Connect (Tabular Iceberg Sink)

See [`docs/benchmark-results/iceberg.md`](../../../../docs/benchmark-results/iceberg.md) for results.

## Prerequisites

- Docker with Compose

## Infrastructure

```bash
task infra:up     # start MinIO + Iceberg REST catalog
task infra:down   # stop and remove all containers
task infra:reset  # wipe data and restart
task infra:logs   # follow container logs
```

MinIO console: http://localhost:9001 (admin/password)
Iceberg REST catalog: http://localhost:8181

## Running

### Generate → Iceberg (no Kafka)

```bash
task bench CORES=4 BATCH=5000 COUNT=1000000
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CORES`   | unbounded | `GOMAXPROCS` |
| `BATCH`   | 1000 | `batching.count` |
| `COUNT`   | 0 (unlimited) | number of messages |

### Varying max_in_flight

```bash
task bench:mif CORES=4 BATCH=10000 MIF=32 COUNT=1000000
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MIF`     | 4 | `max_in_flight` |

### Clean run

```bash
task bench:reset CORES=4 BATCH=5000 COUNT=1000000
```

Wipes all Iceberg data, restarts infrastructure, then runs the benchmark.

## Output

```
INFO rolling stats: 5000 msg/sec, 3.2 MB/sec    @service=redpanda-connect ...
```

Throughput is measured at the pipeline processor, before the Iceberg writer.
