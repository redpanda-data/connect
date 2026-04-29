# Redpanda Connect S3 Sink — Benchmark

Benchmarks Redpanda Connect (`kafka_franz` input → `aws_s3` output) reading from Kafka and writing to S3 (LocalStack). Results are directly comparable to the sibling [Kafka Connect benchmark](../kafka-connect/).

See [`docs/benchmark-results/aws-s3.md`](../../../../../docs/benchmark-results/aws-s3.md) for results and comparison with Kafka Connect.

## Architecture

```
Redpanda Connect producer → bench-events (16 partitions) → Redpanda Connect pipeline → LocalStack S3
```

- **Kafka** — KRaft broker, no ZooKeeper (`confluentinc/cp-kafka:7.7.8`)
- **LocalStack** — local S3 endpoint (`bench-events` bucket)
- **Pipeline** — runs on host via `go run`; reads from Kafka with `kafka_franz`, writes batched JSON to S3 with `aws_s3`
- **Message format** — plain JSON, `archive: lines` batching (one file per flush)

## Prerequisites

- Docker
- `task` (Taskfile runner)
- `go` (to run producer and pipeline)

## Quickstart

```bash
task up
task bench:run COUNT=3000000
task down
```

## Pipeline Parameters

All parameters are configurable as Taskfile vars:

| Var | Default | Pipeline field | Description |
|---|---|---|---|
| `THREADS` | `1` | `pipeline.threads` | Parallel output goroutines |
| `FLUSH_SIZE` | `10000` | `batching.count` | Records per S3 object |
| `FLUSH_PERIOD` | `10s` | `batching.period` | Time-based flush (equivalent to `rotate.schedule.interval.ms`) |
| `FETCH_MIN_BYTES` | `1048576` | `fetch_min_bytes` | Min bytes before broker responds (1 MiB) |

Pass any of them to `bench:run`:

```bash
task bench:run COUNT=3000000 FLUSH_SIZE=50000 THREADS=4
```

## Matrix Benchmark

Loads data once and runs every combination of `THREADS × FLUSH_SIZE × FETCH_MIN_BYTES`:

```bash
task bench:matrix COUNT=3000000
```

Default lists (24 combinations):

| Var | Default values |
|---|---|
| `THREADS_LIST` | `1 2 4 8` |
| `FLUSH_LIST` | `5000 10000 50000` |
| `FETCH_MIN_LIST` | `1048576 4194304` |

Override any list to narrow the sweep:

```bash
task bench:matrix COUNT=3000000 THREADS_LIST="1 4" FLUSH_LIST="10000 50000"
```

Each combination gets a unique consumer group (`rpc-rpc-bench-NNN`) starting from offset 0, so data is loaded once and all combinations read the same messages.

## Comparison with Kafka Connect

| Parameter | Redpanda Connect | Kafka Connect equivalent |
|---|---|---|
| `THREADS` | `pipeline.threads` | `tasks.max` |
| `FLUSH_SIZE` | `batching.count` | `flush.size` |
| `FLUSH_PERIOD` | `batching.period` | `rotate.schedule.interval.ms` |
| `FETCH_MIN_BYTES` | `fetch_min_bytes` | `consumer.override.fetch.min.bytes` |

Key architectural difference: Redpanda Connect runs as a single process on the host, handling all 16 partitions through the `kafka_franz` consumer (which uses one goroutine per partition internally). Kafka Connect runs as N separate JVM tasks each owning a partition subset.

## Tasks Reference

| Task | Description |
|---|---|
| `task up` | Start Kafka and LocalStack, wait for readiness |
| `task down` | Stop and remove all containers and volumes |
| `task bench:run COUNT=N [params]` | Full single run: reset, load, measure, stop |
| `task bench:matrix COUNT=N [lists]` | Sweep all combinations, print table |
| `task pipeline:start [params]` | Start pipeline in background |
| `task pipeline:stop` | Stop background pipeline |
| `task pipeline:logs` | Tail pipeline log (`/tmp/rpc-bench.log`) |
| `task topic:reset` | Delete and recreate `bench-events` topic |
| `task data:load COUNT=N` | Produce N events to bench-events |
| `task bench:lag` | Show current consumer group lag |
| `task bench:offsets` | Show end offsets for bench-events |
| `task bench:s3-count` | Count objects written to S3 bucket |
| `task logs:connect` | Tail Redpanda Connect pipeline log |
| `task logs:localstack` | Follow LocalStack container logs |
