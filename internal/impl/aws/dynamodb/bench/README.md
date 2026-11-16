# Benchmarking DynamoDB CDC Component

Benchmark demonstrating throughput of Redpanda's DynamoDB CDC Connector.

## Prerequisites

Docker (for DynamoDB Local) and Go (already required to build the project).

## How to Run

From the repository root:

```bash
go run ./cmd/tools/bench run aws_dynamodb_cdc
```

This starts DynamoDB Local, creates the tables, seeds 450k items (3 tables × 150k), runs the benchmark until the streams are drained, removes the container, and prints the result. The samples and the summary are written to `target/bench/aws_dynamodb_cdc/streams/<timestamp>/result.json`. See [`cmd/tools/bench`](../../../../../cmd/tools/bench/README.md) for the flags and the result schema.

To watch the rolling stats yourself instead, run `task run` in this directory and stop it with Ctrl-C.

### Re-running

To run the benchmark again without re-seeding, keep the container up the first time:

```bash
go run ./cmd/tools/bench run --keep aws_dynamodb_cdc
go run ./cmd/tools/bench run --skip-setup aws_dynamodb_cdc
```

### Individual tasks

```bash
task bench:up         # start the container and create the tables
task bench:load       # seed all tables in parallel
task bench:reset      # drop the checkpoint table between runs
task bench:run        # run the pipeline until interrupted
task bench:down       # stop and remove the container
```

## Notes

- DynamoDB Streams retain records for **24 hours**. Insert data and run the benchmark promptly.
- DynamoDB Local runs in-memory (`-inMemory` flag), so data is lost on container restart.
- To re-run a benchmark: `task bench:reset` then `task bench:run`.
- To reset all data: `task bench:up`, which replaces the container.

### Expected Output

```
INFO rolling stats: 99000 msg/sec, 204 MB/sec    @service=redpanda-connect bytes/sec=2.03882848e+08 label="" msg/sec=99000 path=root.output.processors.0
INFO rolling stats: 95516 msg/sec, 198 MB/sec    @service=redpanda-connect bytes/sec=1.97727183e+08 label="" msg/sec=95516 path=root.output.processors.0
INFO rolling stats: 102000 msg/sec, 216 MB/sec   @service=redpanda-connect bytes/sec=2.1581314e+08 label="" msg/sec=102000 path=root.output.processors.0
```

> **Note:** DynamoDB Local uses a single shard per table. With 3 tables the connector fully saturates each shard. After all records are consumed throughput drops to 0 until new writes arrive. Real AWS DynamoDB scales horizontally with multiple shards per table.

## Recording Results

After running the benchmark, record your results in [`docs/benchmark-results/dynamodb-cdc.md`](../../../../../docs/benchmark-results/dynamodb-cdc.md). Append a new dated section with environment details, dataset, throughput numbers, and observations. See [`docs/benchmarking.md`](../../../../../docs/benchmarking.md) for the full guide.

