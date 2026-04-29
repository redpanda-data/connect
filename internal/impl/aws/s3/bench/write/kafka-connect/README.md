# Kafka Connect S3 Sink — Benchmark

Benchmarks Kafka Connect (Confluent S3 Sink) reading from Kafka and writing to S3 (LocalStack).

See [`docs/benchmark-results/aws-s3.md`](../../../../../docs/benchmark-results/aws-s3.md) for results and comparison with Redpanda Connect.

## Architecture

```
Redpanda Connect producer → bench-events (16 partitions) → Kafka Connect S3 Sink → LocalStack S3
```

- **Kafka** — KRaft broker, no ZooKeeper (`confluentinc/cp-kafka:7.7.8`)
- **LocalStack** — local S3 endpoint (`bench-events` bucket)
- **Kafka Connect** — S3 Sink connector (`confluentinc/kafka-connect-s3:10.5.23`)
- **Message format** — plain JSON, one record per line per S3 object

## Prerequisites

- Docker
- `task` (Taskfile runner)
- `jq`
- `aws` CLI (for `bench:s3-count` and bucket cleanup)
- `go` (to run the producer)

## Quickstart

```bash
task up
task bench:run COUNT=10000000
task down
```

## Connector Parameters

All parameters are configurable as Taskfile vars:

| Var | Default | Connector field | Description |
|---|---|---|---|
| `TASKS_MAX` | `16` | `tasks.max` | Parallel sink tasks (capped at partition count) |
| `FLUSH_SIZE` | `10000` | `flush.size` | Records per S3 object |
| `POLL_RECORDS` | `5000` | `consumer.override.max.poll.records` | Records fetched per Kafka poll |
| `FETCH_MIN_BYTES` | `1048576` | `consumer.override.fetch.min.bytes` | Min bytes before broker responds (1 MiB) |
| `PART_SIZE` | `67108864` | `s3.part.size` | S3 multipart upload chunk (64 MiB) |

Pass any of them to `connector:create` or `bench:run`:

```bash
task bench:run COUNT=1000000 TASKS_MAX=8 FLUSH_SIZE=50000 POLL_RECORDS=10000
```

## Matrix Benchmark

Run every combination of the parameter lists and get a comparison table:

```bash
task bench:matrix COUNT=1000000
```

Default lists (54 combinations):

| Var | Default values |
|---|---|
| `TASKS_LIST` | `4 8 16` |
| `FLUSH_LIST` | `1000 10000 50000` |
| `POLL_LIST` | `1000 5000 10000` |
| `FETCH_MIN_LIST` | `1048576 4194304` |

Override any list to narrow the sweep:

```bash
task bench:matrix COUNT=1000000 TASKS_LIST="8 16" FLUSH_LIST="10000 50000" POLL_LIST="5000"
```

Each combination gets a unique connector name (and therefore its own consumer group starting from offset 0), so data is loaded only once and all combinations run against the same dataset.

Sample output:

```
════════════════════════════════════════════════════════════════
 Matrix Results (18 combinations, COUNT=1000000)
════════════════════════════════════════════════════════════════
TASKS    FLUSH      POLL     FETCH_MIN    ELAPSED(s)   MSG/S
4        1000       1000     1MB          48           20833
4        1000       5000     1MB          39           25641
4        10000      5000     1MB          31           32258
8        10000      5000     1MB          18           55555
16       50000      10000    4MB          11           90909
...
```

## Tasks Reference

| Task | Description |
|---|---|
| `task up` | Build and start all containers, wait for Connect readiness |
| `task down` | Stop and remove all containers and volumes |
| `task bench:run COUNT=N [params]` | Single parameterised run |
| `task bench:matrix COUNT=N [lists]` | Sweep all combinations, print table |
| `task bench:measure TOTAL=N INTERVAL=S` | Poll lag and print msg/s until drained |
| `task bench:lag` | Show current consumer group lag |
| `task bench:offsets` | Show end offsets for bench-events |
| `task bench:s3-count` | Count objects written to the S3 bucket |
| `task connector:create [params]` | Register connector with current var values |
| `task connector:status` | Show connector and task status |
| `task connector:delete` | Delete the connector (keeps infrastructure running) |
| `task data:load COUNT=N` | Produce N events to bench-events |
| `task logs:connect` | Follow Kafka Connect worker logs |
| `task logs:localstack` | Follow LocalStack logs |
