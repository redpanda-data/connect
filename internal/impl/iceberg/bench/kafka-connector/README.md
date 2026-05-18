# Kafka → Iceberg Benchmark (Kafka Connect vs Redpanda Connect)

End-to-end benchmark comparing **Kafka Connect (Tabular Iceberg Sink)** against **Redpanda Connect** writing from Kafka to Iceberg.

See [`docs/benchmark-results/iceberg.md`](../../../../../docs/benchmark-results/iceberg.md) for results.

## Architecture

```
producer → bench-events → RPCN transformer → bench-events-transformed → Kafka Connect Iceberg Sink → Iceberg
```

The RPCN transformer step is required because the Tabular Iceberg sink connector cannot apply field transformations natively. Redpanda Connect handles the same pipeline in a single process with no intermediate topic.

## Prerequisites

- Docker with Compose
- Go toolchain
- `task` CLI
- `jq`

## Infrastructure

```bash
task up     # start Kafka, MinIO, Iceberg REST, Kafka Connect
task down   # stop and remove all containers and volumes
```

| Service | URL |
|---|---|
| Kafka | localhost:9092 |
| Kafka Connect | http://localhost:8083 |
| Iceberg REST catalog | http://localhost:18181 |
| MinIO console | http://localhost:19001 (admin/password) |

## Quickstart

```bash
task up
task bench:run COUNT=10000000
```

## Tasks Reference

| Task | Description |
|---|---|
| `task up` | Start all containers, wait for Connect readiness |
| `task down` | Stop and remove all containers and volumes |
| `task bench:run COUNT=N` | Full benchmark: produce → transform → sink |
| `task bench:transform` | Phase 1: transform bench-events → bench-events-transformed |
| `task bench:sink` | Phase 2: register connector and measure sink throughput |
| `task bench:measure TOTAL=N INTERVAL=S` | Poll lag and print msg/s until drained |
| `task bench:lag` | Show current consumer group lag |
| `task bench:offsets` | Show end offsets for both topics |
| `task connector:create` | Register the Iceberg sink connector |
| `task connector:status` | Show connector and task status |
| `task connector:delete` | Delete the connector (keeps infrastructure running) |
| `task data:load COUNT=N` | Produce N events to bench-events |
| `task data:reset` | Drop and recreate the Iceberg benchmark.events table |
| `task data:stats` | Show Iceberg table snapshot stats |
| `task control-topic:reset` | Reset the Iceberg control topic |
| `task bench-topic:recreate` | Delete and recreate bench-events topic |
| `task logs:connect` | Follow Kafka Connect worker logs |

## Redpanda Connect Benchmark

Run from the parent folder ([`bench/`](../)):

```bash
cd ..
task bench:run COUNT=10000000
```

Single pipeline — no intermediate topic, no separate transform step.
