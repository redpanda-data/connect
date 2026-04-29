# Redpanda Connect S3 Sink — Benchmark

Benchmarks Redpanda Connect (`kafka_franz` input → `aws_s3` output) reading from Kafka and writing to S3 (LocalStack). Results are directly comparable to the sibling [Kafka Connect benchmark](../kafka-connect/).

See [`docs/benchmark-results/aws-s3.md`](../../../../../docs/benchmark-results/aws-s3.md) for results and comparison with Kafka Connect.

## Architecture

```
Redpanda Connect producer → bench-events (16 partitions) → N pipeline instances → LocalStack S3
```

- **Kafka** — KRaft broker, no ZooKeeper (`confluentinc/cp-kafka:7.7.8`)
- **LocalStack** — local S3 endpoint (`bench-events` bucket)
- **Pipeline** — pre-built Go binary; N instances share the same consumer group, so Kafka distributes the 16 partitions across them (equivalent to Kafka Connect `tasks.max`)
- **Message format** — plain JSON, `archive: lines` batching (one file per flush)

## Prerequisites

- Docker
- `task` (Taskfile runner)
- `go` (to build producer and pipeline)

## Quickstart

```bash
task up
task bench:run COUNT=3000000
task down
```

## Pipeline Parameters

All parameters are configurable as Taskfile vars:

| Var | Default | Description |
|---|---|---|
| `INSTANCES` | `1` | Number of pipeline processes (Kafka distributes partitions across them) |
| `FLUSH_SIZE` | `10000` | Records per S3 object (`batching.count`) |
| `FLUSH_PERIOD` | `10s` | Time-based flush (`batching.period`) |
| `FETCH_MIN_BYTES` | `1048576` | Min bytes before broker responds (1 MiB) |

Pass any of them to `bench:run`:

```bash
task bench:run COUNT=3000000 FLUSH_SIZE=50000 INSTANCES=4
```

## Matrix Benchmark

Loads data once and runs every combination of `INSTANCES × FLUSH_SIZE × FETCH_MIN_BYTES`:

```bash
task bench:matrix COUNT=3000000
```

Default lists (24 combinations):

| Var | Default values |
|---|---|
| `INSTANCES_LIST` | `1 2 4 8` |
| `FLUSH_LIST` | `5000 10000 50000` |
| `FETCH_MIN_LIST` | `1048576 4194304` |

Override any list to narrow the sweep:

```bash
task bench:matrix COUNT=3000000 INSTANCES_LIST="1 2 4" FLUSH_LIST="10000 50000"
```

Each combination gets a unique consumer group (`rpc-rpc-bench-NNN`) starting from offset 0, so data is loaded once and all combinations read the same messages.

## Comparison with Kafka Connect

| Parameter | Redpanda Connect | Kafka Connect equivalent |
|---|---|---|
| `INSTANCES` | N pipeline processes, same consumer group | `tasks.max` |
| `FLUSH_SIZE` | `batching.count` | `flush.size` |
| `FLUSH_PERIOD` | `batching.period` | `rotate.schedule.interval.ms` |
| `FETCH_MIN_BYTES` | `fetch_min_bytes` | `consumer.override.fetch.min.bytes` |

Key architectural difference: each Redpanda Connect instance is a single Go process. Kafka distributes the 16 topic partitions evenly across all instances sharing the consumer group. Kafka Connect runs as N separate JVM tasks each owning a partition subset.

## Tasks Reference

| Task | Description |
|---|---|
| `task up` | Start Kafka and LocalStack, wait for readiness |
| `task down` | Stop and remove all containers and volumes |
| `task bench:run COUNT=N [params]` | Full single run: reset, load, measure, stop |
| `task bench:matrix COUNT=N [lists]` | Sweep all combinations, print table |
| `task pipeline:start [params]` | Start pipeline instance(s) in background |
| `task pipeline:stop` | Stop all background pipeline instances |
| `task pipeline:logs` | Tail pipeline log (`/tmp/rpc-bench.log`) |
| `task topic:reset` | Delete and recreate `bench-events` topic |
| `task data:load COUNT=N` | Produce N events to bench-events |
| `task bench:lag` | Show current consumer group lag |
| `task bench:offsets` | Show end offsets for bench-events |
| `task bench:s3-count` | Count objects written to S3 bucket |
| `task logs:connect` | Tail Redpanda Connect pipeline log |
| `task logs:localstack` | Follow LocalStack container logs |
