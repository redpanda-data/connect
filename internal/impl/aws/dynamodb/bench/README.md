# Benchmarking DynamoDB CDC Component

Benchmark demonstrating throughput of Redpanda's DynamoDB CDC Connector.

## Prerequisites

Docker (for DynamoDB Local) and Go (already required to build the project).

## How to Run

```bash
task run
```

This starts DynamoDB Local, creates the tables, seeds 450k items (3 tables × 150k), and runs the benchmark in one shot.

### Re-running

To run the benchmark again without re-seeding:

```bash
task drop-checkpoint
go run ../../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

### Individual tasks

```bash
task dynamodb:up      # start container
task create           # create tables
task seed             # seed all tables in parallel
task drop-checkpoint  # reset checkpoint between runs
task dynamodb:down    # stop and remove container
```

## Notes

- DynamoDB Streams retain records for **24 hours**. Insert data and run the benchmark promptly.
- DynamoDB Local runs in-memory (`-inMemory` flag), so data is lost on container restart.
- To re-run a benchmark: `task drop-checkpoint` then restart Connect.
- To reset all data: `task dynamodb:down && task dynamodb:up && task create`.

### Expected Output

```
INFO rolling stats: 99000 msg/sec, 204 MB/sec    @service=redpanda-connect bytes/sec=2.03882848e+08 label="" msg/sec=99000 path=root.output.processors.0
INFO rolling stats: 95516 msg/sec, 198 MB/sec    @service=redpanda-connect bytes/sec=1.97727183e+08 label="" msg/sec=95516 path=root.output.processors.0
INFO rolling stats: 102000 msg/sec, 216 MB/sec   @service=redpanda-connect bytes/sec=2.1581314e+08 label="" msg/sec=102000 path=root.output.processors.0
```

> **Note:** DynamoDB Local uses a single shard per table. With 3 tables the connector fully saturates each shard. After all records are consumed throughput drops to 0 until new writes arrive. Real AWS DynamoDB scales horizontally with multiple shards per table.

