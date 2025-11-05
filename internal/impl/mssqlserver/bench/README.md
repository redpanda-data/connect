# Benchmarking Microsoft SQL Server CDC Component

Benchmark demonstrating throughput of Redpanda's Microsoft SQL Server CDC Connector

## How to Run

1. Install local sqlcmd:

```bash
brew install sqlcmd
```

2. Create underlying test tables

```bash
task sqlcmd:create
```

3. Add desired test data using one or all of below task commands:

```bash
task sqlcmd:data:products

task sqlcmd:data:cart

task sqlcmd:data:users
```

4. Run Connect with the SQL Server CDC component configured (see `benchmark_config.yaml`)
```bash
go run ../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

5. Clear checkpoint cache after each run

```bash
task sqlcmd:drop-cache
```

This will:

1. Start Microsoft SQL Server container and Redpanda Connect
2. Create database and generate test data
3. Display throughput logs

### Expected Output

```
INFO rolling stats: 91733 msg/sec, 123 MB/sec      @service=redpanda-connect bytes/sec=1.22793538e+08 label="" msg/sec=91733 path=root.output.processors.0
INFO rolling stats: 101267 msg/sec, 136 MB/sec     @service=redpanda-connect bytes/sec=1.35555936e+08 label="" msg/sec=101267 path=root.output.processors.0
INFO rolling stats: 102000 msg/sec, 136 MB/sec     @service=redpanda-connect bytes/sec=1.36537118e+08 label="" msg/sec=102000 path=root.output.processors.0
INFO rolling stats: 104000 msg/sec, 139 MB/sec     @service=redpanda-connect bytes/sec=1.39214558e+08 label="" msg/sec=104000 path=root.output.processors.0
INFO rolling stats: 102000 msg/sec, 136 MB/sec     @service=redpanda-connect bytes/sec=1.36537106e+08 label="" msg/sec=102000 path=root.output.processors.0
```
