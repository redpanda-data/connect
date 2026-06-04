

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
