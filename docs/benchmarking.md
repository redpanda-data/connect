# Benchmarking Redpanda Connect Components

This document describes how to benchmark Redpanda Connect connectors — the standard approach, the tools involved, and how to record and report results.

## Overview

Each connector that needs benchmarking gets a self-contained `bench/` directory inside its implementation package (e.g. `internal/impl/<component>/bench/`). The benchmark suite should be fully reproducible from a single `task` invocation and should measure throughput of the connector under realistic conditions.

The general approach:

1. Stand up the external dependency (database, message broker, etc.) in Docker
2. Generate a realistic dataset
3. Run Redpanda Connect with the connector configured, using the built-in `benchmark` processor to measure throughput
4. Record results (msg/sec, MB/sec) in the PR description

## Directory Structure

Place benchmarking files in `internal/impl/<component>/bench/`:

```
internal/impl/<component>/bench/
├── README.md                # How to run, prerequisites, expected output
├── Taskfile.yaml            # Task runner for orchestration
├── benchmark_config.yaml    # Redpanda Connect pipeline config
├── docker-compose.yml       # (optional) Multi-service setups
├── create.sql               # (optional) Schema creation scripts
├── users.sql                # (optional) Data generation scripts
└── main.go                  # (optional) Programmatic data seeding
```

## Step-by-Step Guide

### 1. Set Up the External Dependency

Use Docker to run the service locally. Define tasks in `Taskfile.yaml` for starting, stopping, and managing the container. Use the same image that production would use — avoid "lite" or "local" variants unless that's the only option (e.g. DynamoDB Local), and document the limitation.

```yaml
version: '3'

tasks:
  service:up:
    cmd: |
      docker run -d \
        --name <service-name> \
        -p <host-port>:<container-port> \
        -e <ENV_VARS> \
        <image>

  service:down:
    cmd: docker rm -fv <service-name>

  service:logs:
    cmd: docker logs -f <service-name>
```

For benchmarks involving multiple services (e.g. source and destination clusters), use a `docker-compose.yml` instead.

**Reproducibility controls** — For consistent results across runs, pin resources in your docker-compose:

- **CPU pinning** (`cpuset`) — Prevents OS scheduling noise. Assign dedicated cores to each container so they don't compete.
- **Memory limits** (`mem_limit`) — Prevents the OOM killer and keeps conditions consistent.
- **Go runtime tuning** — Set `GOMAXPROCS` and `GOMEMLIMIT` on Connect containers to control goroutine scheduling and GC pressure.

See the [migrator benchmark](../internal/impl/redpanda/migrator/bench/docker-compose.yml) for an example that pins source, destination, loader, and migrator to separate CPU sets:

```yaml
  migrator:
    environment:
      GOMAXPROCS: "3"
      GOMEMLIMIT: "3GiB"
    cpuset: "5,6,7"
    mem_limit: 3500M
```

### 2. Generate Test Data

**Dataset design** — Use multiple tables with different schemas (e.g. users, products, orders) rather than one giant table. This is more realistic and matters for CDC connectors where per-table parallelism is a factor. Use realistic row sizes (1-2KB is typical).

There are three approaches depending on the connector:

**SQL scripts** — For database connectors, write SQL scripts that generate bulk data. Use stored procedures with loops for large datasets:

```sql
-- Example: generate 500,000 rows
DECLARE @i INT = 0;
WHILE @i < 500000
BEGIN
    INSERT INTO users (name, email, created_at)
    VALUES (CONCAT('user-', @i), CONCAT('user', @i, '@example.com'), GETDATE());
    SET @i = @i + 1;
END
```

Add Taskfile entries for each data generation script:

```yaml
  data:users:
    cmd: task sqlcmd EXTRA_ARGS="-i users.sql"
```

**Go seeder program** — For services with native Go SDKs (e.g. DynamoDB), write a `main.go` that seeds data using concurrent workers. Use `BatchWriteItem` or equivalent bulk APIs for speed. See the [DynamoDB benchmark](../internal/impl/aws/dynamodb/bench/main.go) for a reference implementation using 16 concurrent workers to insert 450k items.

**Bloblang `generate` input** — For benchmarks that just need raw message throughput (e.g. migrator benchmarks), use a Redpanda Connect config with `generate` input:

```yaml
input:
  generate:
    interval: ""          # As fast as possible
    count: 30_000_000
    batch_size: 1_000
    mapping: |
      root = "<your payload here>"
```

### 3. Configure the Benchmark Pipeline

Create `benchmark_config.yaml` — a Redpanda Connect config that reads from the connector under test and sinks to `drop: {}` (discard output). The key element is the **`benchmark` processor** which logs rolling throughput statistics:

```yaml
http:
  debug_endpoints: true    # Required for profiling

input:
  <your_connector>:
    # connector-specific config
    batching:
      count: 1000          # Tune batch size for throughput

output:
  processors:
    - benchmark:
        interval: 1s       # How often to log stats
        count_bytes: true   # Report MB/sec in addition to msg/sec
  drop: {}                  # Discard output — we only care about read throughput

logger:
  level: INFO

metrics:
  prometheus:
    add_process_metrics: true
    add_go_metrics: true
```

Key configuration points:

- **`http.debug_endpoints: true`** — Exposes pprof endpoints at `localhost:4195` for CPU/memory/blocking profiling
- **`benchmark` processor** — Logs `msg/sec` and `bytes/sec` at the configured interval
- **`drop: {}`** — Eliminates output overhead so you measure only input throughput
- **Prometheus metrics** — Enables process and Go runtime metrics for monitoring via Grafana

**Batch size tuning** — The `batching.count` parameter has a significant impact on throughput and varies widely across connectors. Existing benchmarks range from 1,000 (SQL Server, DynamoDB) to 140,000 (Oracle CDC). Experiment with this value — too small means excessive per-batch overhead, too large means memory pressure and latency spikes. Document what you tested and what worked best.

**Docker image architecture** — On Apple Silicon (ARM), make sure you're using the correct image architecture. The migrator benchmark explicitly uses `redpandadata/connect:edge-arm64`. Running an x86 image under Rosetta/QEMU emulation will tank throughput numbers and produce misleading results.

### 4. Run the Benchmark

Wire everything together in the Taskfile so `task` (or `task run`) executes the full sequence:

```yaml
  run:
    cmds:
      - task: service:up
      - task: create
      - task: seed
      - go run ../../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

Or for manual step-by-step execution:

```bash
# Start the service
task service:up

# Create schema and seed data
task create
task seed

# Run the benchmark
go run ../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

You should see rolling throughput logs:

```
INFO rolling stats: 101000 msg/sec, 135 MB/sec  @service=redpanda-connect ...
INFO rolling stats: 104000 msg/sec, 139 MB/sec  @service=redpanda-connect ...
```

### 5. Reset Between Runs

Most CDC connectors maintain a checkpoint/cursor. Add a task to clear it between runs:

```yaml
  drop-checkpoint:
    cmd: <command to drop checkpoint table/cache>
```

### 6. Write the README

Every `bench/` directory must have a `README.md` that includes:

1. **Prerequisites** — tools to install (e.g. `brew install sqlcmd`, Docker, etc.)
2. **How to Run** — step-by-step commands
3. **Expected Output** — sample throughput logs so reviewers know what "good" looks like
4. **Notes** — any caveats (e.g. single-shard limitations, container resource constraints, data retention windows)

## Snapshot vs Streaming

For CDC connectors, benchmark **both modes separately** — they have very different performance characteristics:

- **Snapshot mode** — Reads the full current state of tables. Benefits from greater read concurrency and typically achieves higher throughput. Behaves similarly across SQL-based connectors since it's essentially a bulk `SELECT`. Oracle CDC snapshot hit ~140K msg/sec vs ~50K for streaming.
- **Streaming mode** — Reads change events from a log (CDC tables, LogMiner, DynamoDB Streams, etc.). Often single-threaded per table and constrained by the source system's change capture mechanism. This is the mode that matters most for production workloads.

Report both numbers. Snapshot throughput establishes a ceiling; streaming throughput is what customers will actually experience.

## Data Retention and Timing

Some source systems have retention windows for change data:

- **DynamoDB Streams** — 24 hour retention. Insert data and run the benchmark promptly.
- **Oracle LogMiner** — SCN windows and redo log retention. Configure RMAN archive log policies appropriately (see `rman_setup.rman` in the Oracle benchmark).
- **SQL Server CDC** — Cleanup jobs may purge change tables. Disable or extend the retention period for benchmarking.

Document any retention-related constraints in the benchmark README so others don't waste time debugging "0 msg/sec" output.

## Profiling

For deeper investigation, use the profiling tools in `resources/docker/profiling/`:

```bash
# Start Prometheus + Grafana monitoring stack
cd resources/docker/profiling
task up
# Grafana: http://localhost:3000
# Prometheus: http://localhost:9090

# Capture profiles (requires debug_endpoints: true in your config)
task profile:cpu    # 30s CPU profile
task profile:mem    # Memory heap profile
task profile:block  # Goroutine blocking profile

# View profiles in browser
task pprof:cpu
task pprof:mem
task pprof:block
```

For long-running profiling sessions, consider a streaming data generator that produces continuous load (see the migrator's `loader-streaming.yaml` which generates ~100MB/s indefinitely).

## Reporting Results

Record benchmark results **in the PR description**. Include:

- **Runtime environment** — laptop/VM specs, OS, Docker resource limits
- **Dataset** — row count, approximate size (e.g. "1.4KB × 21M rows = 24GB")
- **Throughput** — rolling stats output showing msg/sec and MB/sec
- **Profiling artifacts** — screenshots of Grafana dashboards, Go runtime metrics, memory profiles
- **Observations** — bottleneck analysis, what was tried to improve performance, comparison with other tools if relevant

### Bottleneck Analysis

Good benchmark PRs don't just report numbers — they investigate where the bottleneck is. Techniques used in past benchmarks:

- **Bypass the connector** — Use `sql_raw` input or a simpler input to rule out the connector's own code as the bottleneck vs the source system (done in the SQL Server CDC benchmark).
- **Check connection utilization** — Log `sql.DBStats` to see how many connections are actually in use. The SQL Server benchmark revealed only 1 of 100 connections was active, proving the bottleneck was single-threaded reads, not Connect.
- **Vary the environment** — Test against local Docker, native installs, and cloud-hosted instances (e.g. Azure SQL Premium) to isolate whether containerization overhead matters.
- **Parallelize across tables** — If the source is single-connection-bound per table, test with multiple tables to see if throughput scales linearly with connections.
- **Compare with competitors** — A quick run with Debezium or an equivalent tool establishes whether throughput limits are inherent to the protocol (e.g. Oracle LogMiner) or specific to Connect's implementation.

### Persisting Results to File

For post-hoc analysis, you can write benchmark output to a file instead of (or in addition to) dropping it:

```yaml
output:
  processors:
    - benchmark:
        interval: 1s
        count_bytes: true
  file:
    path: "./results.json"
    codec: lines
```

### Recording Results

In addition to the PR description, add or update a results file in [`docs/benchmark-results/`](benchmark-results/). Each connector gets its own file (e.g. `mssqlserver-cdc.md`). Append new runs as dated sections so we can track performance over time.

When adding a new result, include:
- Date and PR link
- Environment details (hardware, Docker config, resource limits)
- Dataset description (row count, row size, total size)
- Configuration highlights (batch size, parallelism, tuning parameters)
- Throughput table and raw log output
- Observations and bottleneck analysis

Example from the SQL Server CDC benchmark PR:

> Runtime: ~4m 30s
> Dataset: 1.4kb × 21,198,489 rows = 24.1GB
>
> ```
> INFO rolling stats: 101000 msg/sec, 135 MB/sec
> INFO rolling stats: 104000 msg/sec, 139 MB/sec
> INFO rolling stats: 103000 msg/sec, 138 MB/sec
> ```

## Existing Benchmarks

For a non-technical overview suitable for sales, marketing, and other non-engineering audiences, see the [Performance Summary](benchmark-results/SUMMARY.md).

| Component | Bench Suite | Results | Throughput | Notes |
|---|---|---|---|---|
| Redpanda Migrator | [`internal/impl/redpanda/migrator/bench/`](../internal/impl/redpanda/migrator/bench/) | [results](benchmark-results/redpanda-migrator.md) | 1 GB/s+, 1M msg/sec | Cluster-to-cluster, 30GB transfer |
| SQL Server CDC | [`internal/impl/mssqlserver/bench/`](../internal/impl/mssqlserver/bench/) | [results](benchmark-results/mssqlserver-cdc.md) | ~135 MB/sec, 100K msg/sec | Single connection bottleneck |
| Oracle CDC | [`internal/impl/oracledb/bench/`](../internal/impl/oracledb/bench/) | [results](benchmark-results/oracledb-cdc.md) | ~50K msg/sec (streaming) | LogMiner single-threaded limitation |
| DynamoDB CDC | [`internal/impl/aws/dynamodb/bench/`](../internal/impl/aws/dynamodb/bench/) | [results](benchmark-results/dynamodb-cdc.md) | ~200 MB/sec, 100K msg/sec | DynamoDB Local, 3 tables x 150K items |

## Keeping Results Up to Date

Benchmark results go stale. Follow these practices to keep them current:

1. **When adding a new benchmark suite** — Create a corresponding results file in `docs/benchmark-results/`, update the table in this document, and update `docs/benchmark-results/SUMMARY.md`.

2. **When modifying a connector's performance path** — Re-run the benchmark and append a new dated section to the results file. This includes changes to batching, buffering, connection handling, serialization, or any code that sits in the hot path.

3. **When re-running an existing benchmark** — Always append (don't replace) so we can track performance over time. Include the date, PR link, and what changed since the last run.

4. **During code review** — The `/review` skill includes a benchmarking check. It will flag PRs that add or modify `bench/` directories without updating results files, and PRs that include throughput numbers in the description without recording them in `docs/benchmark-results/`. It will also note when performance-critical connector changes may warrant a benchmark re-run.

## Go Benchmark Tests

For unit-level benchmarks of internal components (serialization, conversion, etc.), use standard Go `testing.B` benchmarks in `*_test.go` files. Use `b.ReportMetric()` to report domain-specific metrics (e.g. spans/sec) and `b.ReportAllocs()` for allocation tracking:

```go
func BenchmarkConvert(b *testing.B) {
    // setup...
    b.ReportAllocs()
    for b.Loop() {
        // operation under test
    }
    b.ReportMetric(float64(itemCount)/b.Elapsed().Seconds(), "items/sec")
}
```

These are complementary to the integration-level benchmarks described above and are useful for isolating performance of specific code paths.
