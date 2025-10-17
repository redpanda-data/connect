# MMT

Install local sqlcmd: brew install sqlcmd

SQL Server


2025-10-17 12:03:41.96 Server      SQL Server detected 1 sockets with 12 cores per socket and 12 logical processors per socket, 12 total logical processors; using 4 logical processors based on SQL Server licensing. This is an informational message; no user action is required.
2025-10-17 12:03:41.97 Server      SQL Server is starting at normal priority base (=7). This is an informational message only. No user action is required.
2025-10-17 12:03:41.97 Server      Detected 9547 MB of RAM. This is an informational message; no user action is required.
2025-10-17 12:03:41.97 Server      Using conventional memory in the memory manager.
2025-10-17 12:03:41.97 Server      Page exclusion bitmap is enabled.
2025-10-17 12:03:42.01 Server      Buffer Pool: Allocating 2097152 bytes for 1455036 hashPages.
2025-10-17 12:03:42.23 Server      Buffer pool extension is already disabled. No action is necessary.
src-1  | 2025/10/17 12:03:42 [launchpadd] WARNING: Failed to connect to SQL because: dial tcp [::1]:1431: connect: connection refused, will reattempt connection.
2025-10-17 12:03:42.99 Server      Successfully initialized the TLS configuration. Allowed TLS protocol versions are ['1.0 1.1 1.2']. Allowed TLS ciphers are ['ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-SHA256:ECDHE-ECDSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA:ECDHE-ECDSA-AES128-SHA:AES256-GCM-SHA384:AES128-GCM-SHA256:AES256-SHA256:AES128-SHA256:AES256-SHA:AES128-SHA:!DHE-RSA-AES256-GCM-SHA384:!DHE-RSA-AES128-GCM-SHA256:!DHE-RSA-AES256-SHA:!DHE-RSA-AES128-SHA'].
2025-10-17 12:03:43.01 Server      Query Store settings initialized with enabled = 1,
2025-10-17 12:03:43.02 Server      Node configuration: node 0: CPU mask: 0x0000000000000fff:0 Active CPU mask: 0x000000000000000f:0. This message provides a description of the NUMA configuration for this computer. This is an informational message only. No user action is required.
2025-10-17 12:03:43.03 Server      Using dynamic lock allocation.  Initial allocation of 500 Lock blocks and 1000 Lock Owner blocks per node.  This is an informational message only.  No user action is required.
2025-10-17 12:03:43.04 Server      Database Instant File Initialization: enabled. For security and performance considerations see the topic 'Database Instant File Initialization' in SQL Server Books Online. This is an informational message only. No user action is required.
ForceFlush is enabled for this instance.

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
