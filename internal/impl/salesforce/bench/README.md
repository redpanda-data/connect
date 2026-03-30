# Benchmarking Salesforce Connector

Measures read throughput of the Salesforce REST snapshot (all SObjects or a subset).

## Prerequisites

Set the following environment variables:

```bash
export SALESFORCE_ORG_URL="https://your-domain.salesforce.com"
export SALESFORCE_CLIENT_ID="your-connected-app-client-id"
export SALESFORCE_CLIENT_SECRET="your-connected-app-client-secret"
```

## Running

### Full snapshot (all SObjects, parallel_fetch=10)

```bash
task bench:run
```

### Single SObject (Account only)

```bash
task bench:run:single
```

### Compare parallel_fetch values

```bash
task bench:run:parallel-1
task bench:run:parallel-5
task bench:run:parallel-20
```

### Clear checkpoint between runs

The connector checkpoints progress so it won't re-read already-fetched records.
Clear it before each benchmark run to always start from scratch:

```bash
task bench:clear-checkpoint
```

## CDC Benchmark

Streams Account change events in real time via the Salesforce Pub/Sub API (gRPC), measuring event delivery throughput.

### Prerequisites

Enable Change Data Capture for Account in your Salesforce org:

> Setup → Change Data Capture → select **Account** → Save

### Running

In one terminal, generate a continuous stream of Account changes:

```bash
task bench:write:realtime
```

In a second terminal, start the CDC reader:

```bash
task bench:cdc
```

To start fresh (replay from the earliest available event):

```bash
task bench:cdc:fresh
```

### Expected Output

```
INFO rolling stats: 800 msg/sec, 1 MB/sec    @service=redpanda-connect ...
INFO rolling stats: 950 msg/sec, 1 MB/sec    @service=redpanda-connect ...
```

CDC throughput is bounded by the Pub/Sub API batch size (`cdc_batch_size`) and the rate at which your org generates change events.

## Write Benchmark

Generates synthetic Account records and upserts them into Salesforce.

### Realtime mode (sObject Collections API, up to 200 records/call)

```bash
task bench:write:realtime
```

### Bulk mode (Bulk API 2.0, async CSV upload)

```bash
task bench:write:bulk
```

## Expected Output

```
INFO rolling stats: 1200 msg/sec, 2 MB/sec    @service=redpanda-connect ...
INFO rolling stats: 1850 msg/sec, 3 MB/sec    @service=redpanda-connect ...
INFO rolling stats: 2100 msg/sec, 3 MB/sec    @service=redpanda-connect ...
```

Throughput depends on:
- Number of records in your org
- `parallel_fetch` value (more = higher throughput, more API quota consumed)
- `query_batch_size` (2000 is the Salesforce max per page)
- Network latency to the Salesforce instance
- SObject field count (wide objects produce larger messages)

## Recording Results

Run the benchmark and record your results in [docs/benchmark-results/salesforce.md](../../../../docs/benchmark-results/salesforce.md), appending a new dated section with environment, dataset, throughput, and observations.
