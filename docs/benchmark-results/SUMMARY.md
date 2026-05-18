# Redpanda Connect Performance Summary

Last updated: 2026-03-16

## At a Glance

| Connector | Peak Throughput | Messages/sec | What Was Tested |
|---|---|---|---|
| **Redpanda Migrator** | **1 GB/s** | 1,035,000 | Cluster-to-cluster data migration (30GB) |
| **DynamoDB CDC** | **216 MB/s** | 102,000 | Change data capture from 3 DynamoDB tables |
| **SQL Server CDC** | **154 MB/s** | 119,000 | Change data capture from SQL Server |
| **Oracle CDC (snapshot)** | — | 140,000 | Initial table snapshot from Oracle DB |
| **Oracle CDC (streaming)** | — | 50,000–90,000 | Real-time change streaming via LogMiner |

## What These Numbers Mean

**Redpanda Migrator** can move data between Redpanda clusters at over 1 gigabyte per second. A 30GB migration completes in roughly 30 seconds.

**DynamoDB CDC** captures changes from DynamoDB tables at up to 216 MB/s. In production with multiple shards per table, throughput can scale further.

**SQL Server CDC** reads change data from SQL Server at up to 154 MB/s. Throughput scales linearly with the number of tables being captured — each table uses its own database connection.

**Oracle CDC** has two operating modes. Snapshot mode (bulk reading existing data) processes ~140,000 messages per second. Streaming mode (real-time changes via Oracle's LogMiner) processes 50,000–90,000 messages per second. The streaming throughput is limited by Oracle's LogMiner subsystem itself, not by Redpanda Connect — competing products (e.g. Debezium) show similar numbers on the same workload.

## Test Conditions

All benchmarks were run on developer laptops with the source databases running in Docker containers. Production deployments on dedicated hardware with properly sized databases will typically perform better.

These numbers represent **read throughput** — how fast Redpanda Connect can ingest data from each source. Write throughput to destination systems depends on the target and is benchmarked separately.

## Detailed Results

For full methodology, raw output, environment details, and bottleneck analysis, see the individual result files:

- [Redpanda Migrator](redpanda-migrator.md)
- [SQL Server CDC](mssqlserver-cdc.md)
- [Oracle CDC](oracledb-cdc.md)
- [DynamoDB CDC](dynamodb-cdc.md)

## How Benchmarks Are Run

See the [Benchmarking Guide](../benchmarking.md) for the full process. In short: each connector has a reproducible benchmark suite that stands up the source system in Docker, generates test data, and measures throughput using Redpanda Connect's built-in benchmark processor.
