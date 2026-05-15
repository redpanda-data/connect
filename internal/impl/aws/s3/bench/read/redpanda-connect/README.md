# Benchmarking AWS S3 Input Component

Benchmark demonstrating read throughput of Redpanda's `aws_s3` input connector.

## Prerequisites

Docker (for LocalStack) and Go (already required to build the project).

## How to Run

```bash
task run
```

This starts LocalStack, creates the `bench-objects` bucket, seeds 50,000 objects (~1 KB each), and runs the benchmark in one shot.

### Re-running

Objects persist in LocalStack for the lifetime of the container, so you can re-run the benchmark without re-seeding:

```bash
go run ../../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

To re-seed with fresh data:

```bash
task drop-bucket
task seed
go run ../../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

### Individual tasks

```bash
task localstack:up    # start LocalStack container
task create           # create the bench-objects bucket
task seed             # upload 50K objects (~1 KB each)
task drop-bucket      # empty the bucket (delete all objects)
task localstack:down  # stop and remove the container
```

### Seed options

```bash
go run . seed --total 100000 --size 4096 --workers 128
```

## Notes

- LocalStack runs all AWS services locally; S3 is available on port 4566.
- The `aws_s3` input walks objects sequentially via `ListObjectsV2` (100 keys per page) and downloads each one. Throughput is dominated by per-object HTTP round trips, not raw data size.
- The benchmark terminates automatically once all objects have been read (`ErrEndOfInput`).
- To increase throughput, try larger objects (`--size`) or more objects. Parallelism in the connector itself is limited to one object at a time.

### Expected Output

```
INFO rolling stats: 3000 msg/sec, 3 MB/sec     @service=redpanda-connect bytes/sec=3.1e+06 label="" msg/sec=3000 path=root.output.processors.0
INFO rolling stats: 3200 msg/sec, 3 MB/sec     @service=redpanda-connect bytes/sec=3.3e+06 label="" msg/sec=3200 path=root.output.processors.0
```

> **Note:** LocalStack runs in-process with no real network overhead. Production S3 throughput will differ depending on network latency, object size, and instance type.

## Recording Results

After running the benchmark, record your results in [`docs/benchmark-results/aws-s3.md`](../../../../../docs/benchmark-results/aws-s3.md). Append a new dated section with environment details, dataset, throughput numbers, and observations. See [`docs/benchmarking.md`](../../../../../docs/benchmarking.md) for the full guide.
