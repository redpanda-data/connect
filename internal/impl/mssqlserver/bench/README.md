# Benchmarking Microsoft SQL Server CDC Component

Benchmark demonstrating throughput of Redpanda's Microsoft SQL Server CDC Connector

```bash
$ task up
```

```bash
$ go run loader/main.go
```

## How to Run

```bash
task
```

This will:
1. Start Microsoft SQL Server container and Redpanda Connect
2. Generate 30GB of test data
3. Display throughput logs

### Expected Output

```
INFO rolling stats: 91733 msg/sec, 123 MB/sec      @service=redpanda-connect bytes/sec=1.22793538e+08 label="" msg/sec=91733 path=root.output.processors.0
INFO rolling stats: 101267 msg/sec, 136 MB/sec     @service=redpanda-connect bytes/sec=1.35555936e+08 label="" msg/sec=101267 path=root.output.processors.0
INFO rolling stats: 102000 msg/sec, 136 MB/sec     @service=redpanda-connect bytes/sec=1.36537118e+08 label="" msg/sec=102000 path=root.output.processors.0
INFO rolling stats: 104000 msg/sec, 139 MB/sec     @service=redpanda-connect bytes/sec=1.39214558e+08 label="" msg/sec=104000 path=root.output.processors.0
INFO rolling stats: 102000 msg/sec, 136 MB/sec     @service=redpanda-connect bytes/sec=1.36537106e+08 label="" msg/sec=102000 path=root.output.processors.0
```

### Useful Commands:

Deleting the internal checkpoint cache to start streaming from the start of the change tables:

```bash
drop table rpcn.CdcCheckpointCache;
```

View how much space each table uses (including change tables)

```bash
sp_msforeachtable 'EXEC sp_spaceused [?]'
```
