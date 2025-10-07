# Redpanda Migrator Benchmark

Benchmark demonstrating the Redpanda migrator achieving **1GB/s+ throughput**.

## Purpose

Measures migrator performance transferring 30GB of data between two Redpanda clusters.

## How to Run

```bash
task
```

This will:
1. Start source and destination Redpanda clusters
2. Generate 30GB of test data
3. Run the migrator
4. Display throughput logs

## Expected Output

```
[output.processors.0] msg="rolling stats: 1035873 msg/sec, 1.0 GB/sec"
[output.processors.0] msg="rolling stats: 1035211.5 msg/sec, 1.0 GB/sec"
[output.processors.0] msg="rolling stats: 1037427.5 msg/sec, 1.0 GB/sec"
```

Migration completes in ~30 seconds.

## Streaming Mode

For long-running profiling, enable streaming mode by editing `docker-compose.yml`:

1. Replace loader config:
   ```yaml
   - ./loader-streaming.yaml:/config.yaml:ro
   ```

2. Change loader condition:
   ```yaml
   condition: service_started
   ```

Streaming mode generates continuous data at 100MB/s, allowing extended profiling sessions.
