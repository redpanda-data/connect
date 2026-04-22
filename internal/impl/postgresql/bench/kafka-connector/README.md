# Kafka Connect JDBC Sink — PostgreSQL Benchmark

Benchmarks Kafka Connect (Confluent JDBC Sink) reading from Kafka and writing to PostgreSQL.

See [`docs/benchmark-results/postgres.md`](../../../../../docs/benchmark-results/postgres.md) for results and comparison with Redpanda Connect.

## Architecture

```
Redpanda Connect producer → bench-events (16 partitions) → Kafka Connect JDBC Sink → PostgreSQL bench_events
```

- **Kafka** — KRaft broker, no ZooKeeper
- **PostgreSQL 16** — sink target (`benchdb.bench_events`)
- **Kafka Connect** — JDBC Sink connector (`confluentinc/kafka-connect-jdbc:10.7.14`)
- **Message format** — JSON with embedded Kafka Connect schema envelope (no Schema Registry)

## Prerequisites

- Docker
- `task` (Taskfile runner)
- `jq`
- `go` (to run the producer)

## Quickstart

```bash
task up
task bench:run COUNT=10000000
task down
```

## Tasks Reference

| Task | Description |
|---|---|
| `task up` | Build and start all containers, wait for Connect readiness |
| `task down` | Stop and remove all containers and volumes |
| `task bench:run COUNT=N` | Full benchmark: produce N events, register connector, measure throughput |
| `task bench:measure TOTAL=N INTERVAL=S` | Poll lag and print msg/s until topic is drained |
| `task bench:lag` | Show current consumer group lag |
| `task bench:offsets` | Show end offsets for bench-events |
| `task bench:pg-count` | Count rows written to bench_events in PostgreSQL |
| `task connector:create` | Register the JDBC sink connector |
| `task connector:status` | Show connector and task status |
| `task connector:delete` | Delete the connector (keeps infrastructure running) |
| `task data:load COUNT=N` | Produce N events to bench-events |
| `task logs:connect` | Follow Kafka Connect worker logs |
| `task logs:postgres` | Follow PostgreSQL logs |

## Connector Configuration

Key settings in `connector.json`:

| Setting | Value | Notes |
|---|---|---|
| `tasks.max` | 16 | Matches topic partition count |
| `insert.mode` | `insert` | Append-only inserts |
| `batch.size` | 3000 | Rows per JDBC batch |
| `table.name.format` | `bench_events` | Explicit table name |
| `value.converter.schemas.enable` | `true` | Reads column mapping from message schema envelope |
