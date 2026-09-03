

## AWS — cdc — 2026-06-04

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`abea8aeba`](https://github.com/redpanda-data/connect/commit/abea8aebaa99ee8846dd8d6f5a2f84e223619962)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |            6 |        5.558 |         3,865 |            4 |           4 |            6 |         3,933 |                    |


Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-04T14-03-56Z.json`](results/dynamodb/cdc/2026-06-04T14-03-56Z.json)


## AWS — cdc — 2026-06-04

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`2a1c0e637`](https://github.com/redpanda-data/connect/commit/2a1c0e637d3445c2634b830c2de21ab6f8d0faed)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           10 |        9.889 |         2,197 |            9 |           6 |           13 |         2,239 |                    |


Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-04T21-11-50Z.json`](results/dynamodb/cdc/2026-06-04T21-11-50Z.json)


## AWS — cdc — 2026-06-04

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`817a6a3c1`](https://github.com/redpanda-data/connect/commit/817a6a3c19e84b04c36aae70495f31dab523a53e)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           18 |       18.022 |         3,997 |           16 |           4 |           31 |         4,086 |                    |
| 2          | connect       |           17 |       15.295 |         3,399 |           16 |          12 |           19 |         3,741 |                    |
| 4          | connect       |           13 |       12.950 |         2,876 |           12 |          12 |           14 |         2,885 |                    |
| 8          | connect       |           13 |       12.966 |         2,877 |           12 |          12 |           14 |         2,882 |                    |


> ⚠ At 2 vCPU: 278s dip to 0.37× median MB/sec from t=533s — investigate before publishing.

> ⚠ At 2 vCPU: 85s dip to 0.56× median MB/sec from t=815s — investigate before publishing.



Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-04T23-22-37Z.json`](results/dynamodb/cdc/2026-06-04T23-22-37Z.json)


## AWS — cdc — 2026-06-05

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`817a6a3c1`](https://github.com/redpanda-data/connect/commit/817a6a3c19e84b04c36aae70495f31dab523a53e)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 8          | connect       |           18 |       18.026 |         3,999 |           16 |          17 |           19 |         4,016 |                    |


Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-05T01-33-20Z.json`](results/dynamodb/cdc/2026-06-05T01-33-20Z.json)


## AWS — cdc — 2026-06-05

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`817a6a3c1`](https://github.com/redpanda-data/connect/commit/817a6a3c19e84b04c36aae70495f31dab523a53e)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           32 |       26.980 |         5,981 |           32 |           7 |           46 |         7,218 |                    |


> ⚠ At 1 vCPU: 228s dip to 0.00× median MB/sec from t=584s — investigate before publishing.

> ⚠ At 1 vCPU: 95s dip to 0.00× median MB/sec from t=815s — investigate before publishing.



Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-05T02-42-38Z.json`](results/dynamodb/cdc/2026-06-05T02-42-38Z.json)


## AWS — cdc — 2026-06-05

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`817a6a3c1`](https://github.com/redpanda-data/connect/commit/817a6a3c19e84b04c36aae70495f31dab523a53e)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           30 |       20.782 |         4,607 |           31 |           0 |           46 |         6,557 |                    |


> ⚠ At 1 vCPU: 299s dip to 0.00× median MB/sec from t=512s — investigate before publishing.

> ⚠ At 1 vCPU: 86s dip to 0.00× median MB/sec from t=813s — investigate before publishing.



Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-05T04-48-51Z.json`](results/dynamodb/cdc/2026-06-05T04-48-51Z.json)


## AWS — cdc — 2026-06-05

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`817a6a3c1`](https://github.com/redpanda-data/connect/commit/817a6a3c19e84b04c36aae70495f31dab523a53e)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           36 |       33.371 |         7,401 |           32 |          12 |           46 |         7,933 |                    |


Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-05T06-01-27Z.json`](results/dynamodb/cdc/2026-06-05T06-01-27Z.json)


## AWS — cdc — 2026-06-05

**Scenario:** Stream changes from a DynamoDB table under sustained PutItem load so the
aws_dynamodb_cdc input — not the producer — is the bottleneck across the
CPU sweep. Drop + recreate the table between sweep points (TRUNCATE doesn't
exist on DDB) to reset the stream ARN and shard state.

Connect-only initially: no Debezium DynamoDB connector ships in the bench
cloud-init, so engines=[connect]. Head-to-head with a paid KC connector
is deferred.

**Git SHA:** [`e2fb34e85`](https://github.com/redpanda-data/connect/commit/e2fb34e859afc85539bd723d328a2c837d027fe6)

**Infra:** Runner `c8g.4xlarge`; source `` (0 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           36 |       34.251 |         7,594 |           32 |          12 |           47 |         8,068 |                    |
| 2          | connect       |           15 |       15.993 |         3,541 |           14 |           1 |           35 |         3,407 |                    |
| 4          | connect       |           12 |       11.868 |         2,630 |           11 |          10 |           13 |         2,716 |                    |
| 8          | connect       |            0 |        0.000 |             0 |            0 |           0 |            0 |             0 |                    |


> ⚠ At 2 vCPU: 61s dip to 0.00× median MB/sec from t=126s — investigate before publishing.



Raw samples + Prometheus snapshots: [`results/dynamodb/cdc/2026-06-05T14-27-06Z.json`](results/dynamodb/cdc/2026-06-05T14-27-06Z.json)


## Investigation findings (2026-06-05)

The four-point sweep above (36 → 15 → 12 → 0 MB/s) is NOT a measurement of
Connect's vCPU scaling — it's an artefact of the bench's reset strategy not
fitting DynamoDB Streams' shard lifecycle. Do not publish this curve as
Connect's "anti-scaling" behaviour without the context below.

### What we observed

| Point | vCPU | Connect MB/s | CPU sec | CPU utilisation | Brokers MB/s |
|-------|------|--------------|---------|-----------------|--------------|
| 1     | 1    | 36           | 926     | 96%             | 31           |
| 2     | 2    | 15           | 472     | 25%             | 16           |
| 4     | 4    | 12           | 343     | 9%              | 11           |
| 8     | 8    | **0**        | **0.75**| **0.08%**       | **EMPTY**    |

At point 8 the Connect process used 0.75 CPU-seconds over a 16-minute window
and produced zero records to Redpanda. Connect was alive but idle, not blocked
on I/O — the goroutine count stayed at 312 (within the normal range), heap
stayed under 100 MB, no log output beyond the initial startup line.

### Hypothesis: stream shard rotation outpaces the connector's discovery

Each sweep point keeps the same source DDB table and stream. The connector
restarts (checkpoint table dropped between points → `start_from: latest`).
After ~50 minutes of sustained 9K PutItems/sec, the stream's shard topology
has rotated heavily:

- Many shards have CLOSED (DDB rotates shards under high write activity)
- New shards have been created to absorb fresh writes
- `ListShards` returns ALL shards (open + closed)
- `GetShardIterator(LATEST)` on a closed shard returns an iterator that yields
  zero records immediately (latest = end of finite shard's data)
- The connector's refresh interval is 30s (see `shardRefreshInterval` in
  `internal/impl/aws/dynamodb/input_cdc.go`)

If the connector at point 8 startup discovers mostly-closed shards, all shard
readers exit quickly with `exhausted=true`. The connector then waits for
`refreshShards` to discover new shards — but if the new shards aren't
appearing in `ListShards` output (or appear with iterator types that yield
nothing), the connector stays idle indefinitely.

This is consistent with every observation: 0 CPU (no active shard readers),
0 records (no shard yields records), clean exit at end of bench window (no
fatal error condition).

### What this means for publishing

- **Connect at 1 vCPU on a fresh stream sustains 36 MB/s** (source-rate-bound
  at the configured 9K PutItems/sec; matches mean_msg_s of 7594).
- **vCPU scaling cannot be measured with the current bench design.** A
  meaningful sweep requires either:
  1. A fresh source table per sweep point (drop+recreate; ~20-30 min per
     reset due to DDB delete latency)
  2. A multi-table workload that distributes writes across stable partitions
  3. Investigation into why `start_from: latest` doesn't reliably pick up new
     shards under sustained write activity (likely an `aws_dynamodb_cdc`
     connector improvement)
- **The DDB account-level 40K WCU per-table quota also limits source rate
  to 40 MB/s** — comparing against the postgres bench's 102 MB/s requires
  either a quota increase or a multi-table workload (DDB best practice
  anyway).

### Other bugs found and fixed during this investigation

1. **cdc-ddb seeder silently capped at 4K PutItems/sec** instead of the
   configured rate — `perWorkerPerTick` was being capped at `dynamoBatchMax`
   (25) without sending multiple BatchWriteItem calls per tick. Fixed at
   `e2fb34e85`.
2. **cdc-ddb seeder workers died on transient AWS errors** —
   `writeBatch` returned the first error from BatchWriteItem instead of
   retrying transient `InternalServerError` / `ProvisionedThroughputExceededException`.
   Worker death cascaded to whole workload exiting, masquerading as a
   connector-side throughput drop. Fixed at `e2fb34e85`.
3. **DynamoDB CreateTable hangs silently on quota-exceeded WCU requests**
   — 120K WCU request sat in CREATING state for 47 minutes. Caught and
   resized to 40K (default quota). 30-minute terraform delete timeout
   added to `terraform/modules/dynamodb-bench/main.tf` (`817a6a3c1`).
4. **`task aws:bench` doesn't forward `--engines=connect`** for
   Connect-only scenarios — must invoke runner directly. Documented in
   `bench-framework-state` memory.

### Falsified hypotheses (don't waste time re-investigating)

- AWS Streams per-shard `GetRecords` throttling — instrumented and verified
  zero throttle events at 8 vCPU on a fresh stream
- GOMAXPROCS-induced lock contention in `RecordBatcher` — 8 vCPU on a
  fresh stream achieves the same 18-36 MB/s as 1 vCPU
- `RecordBatcher.ShouldThrottle()` backpressure from downstream Redpanda —
  instrumented, zero firings observed during the degradation

