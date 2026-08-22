

## AWS — orders-sink-smoke — 2026-08-18

**Scenario:** One-point smoke for the snowflake_streaming sink path: seed a small topic,
drain it into one Snowflake table at 1 vCPU, and verify the whole chain
(SSM params -> TF outputs -> key fetch -> tablegen reset -> Snowpipe
Streaming ingest -> SHOW TABLES polling) before committing to the full
sweep. Also validates the known risk: SHOW TABLES BYTES may lag streaming
commits, in which case ROW_COUNT (total_records) is the dependable signal.
Connect-only — no Kafka Connect counterpart is wired for this sink.

One-time setup (SSM params + Snowflake user/key-pair) is documented in
scenarios/snowflake/README.md.

**Git SHA:** [`afb3faf07`](https://github.com/redpanda-data/connect/commit/afb3faf07526bc12f4f4e4a199370a4a4ca84260)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |            0 |        0.047 |            41 |            0 |           0 |            0 |            37 |                    |


Raw samples + Prometheus snapshots: [`results/snowflake/orders-sink-smoke/2026-08-18T19-32-30Z.json`](results/snowflake/orders-sink-smoke/2026-08-18T19-32-30Z.json)


## AWS — orders-sink-smoke — 2026-08-18

**Scenario:** One-point smoke for the snowflake_streaming sink path: seed a small topic,
drain it into one Snowflake table at 1 vCPU, and verify the whole chain
(SSM params -> TF outputs -> key fetch -> tablegen reset -> Snowpipe
Streaming ingest -> SHOW TABLES polling) before committing to the full
sweep. Also validates the known risk: SHOW TABLES BYTES may lag streaming
commits, in which case ROW_COUNT (total_records) is the dependable signal.
Connect-only — no Kafka Connect counterpart is wired for this sink.

One-time setup (SSM params + Snowflake user/key-pair) is documented in
scenarios/snowflake/README.md.

**Git SHA:** [`b407c7166`](https://github.com/redpanda-data/connect/commit/b407c71666ee1a117a3b69ed06771f2a53e66864)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 12,000,000 rows × 1200 B = ~13 GB

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |           15 |       12.333 |        11,077 |           15 |           3 |           23 |        13,638 |                    |


Raw samples + Prometheus snapshots: [`results/snowflake/orders-sink-smoke/2026-08-18T19-55-03Z.json`](results/snowflake/orders-sink-smoke/2026-08-18T19-55-03Z.json)


## AWS — orders-sink — 2026-08-18

**Scenario:** Drain a pre-seeded Redpanda topic of flat JSON records into a Snowflake
table via the snowflake_streaming output (Snowpipe Streaming), across a
vCPU sweep. Throughput is server-side committed truth: snowflake-tablegen
polls SHOW TABLES (metadata-only, no warehouse credits) for the table's
bytes/rows every 10s. Bounded dataset (no sustained workload): the topic is
the fixed input; each sweep point re-reads it from the beginning against a
freshly CREATE OR REPLACEd table. Connect-only — no Kafka Connect
counterpart is wired for this sink.

Run the smoke scenario (orders-sink-smoke.yaml) first on a fresh setup;
one-time SSM/Snowflake setup is in scenarios/snowflake/README.md.

**Git SHA:** [`274696db0`](https://github.com/redpanda-data/connect/commit/274696db01e07ac77f7ca78d468d3bec23e1f850)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 160,000,000 rows × 1200 B = ~178 GB

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |           20 |       17.429 |        15,653 |           20 |           5 |           22 |        18,185 |                    |
| 2    | 2          |                | connect       |           66 |       62.692 |        56,277 |           66 |          39 |           77 |        58,884 |                    |
| 4    | 4          |                | connect       |           61 |       57.248 |        51,390 |           61 |          29 |           81 |        54,560 |                    |
| 8    | 8          |                | connect       |           71 |       70.622 |        63,393 |           71 |          49 |           91 |        63,655 |                    |


Raw samples + Prometheus snapshots: [`results/snowflake/orders-sink/2026-08-18T20-32-57Z.json`](results/snowflake/orders-sink/2026-08-18T20-32-57Z.json)
