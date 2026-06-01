

## AWS — orders-cdc — 2026-06-01

**Scenario:** Stream changes from a high-write MySQL orders table (target 150K writes/sec
≈ 180 MB/s) so the mysql_cdc input — not the producer — is the bottleneck
across the whole CPU sweep. TRUNCATE between sweep points keeps the table
size bounded (no Trap 3).

**Git SHA:** [`77ecd3ad8`](https://github.com/redpanda-data/connect/commit/77ecd3ad85d5f2734a42f41901ef07701ca03926)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.2xlarge` (400 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           64 |            4 |          62 |           70 |        50,000 |                    |
| 1          | kafka_connect |           34 |           34 |          32 |           35 |             0 | -30 MB/s (-47%)    |
| 2          | connect       |           95 |            6 |          93 |          101 |        75,000 |                    |
| 2          | kafka_connect |           38 |           38 |          33 |           39 |             0 | -57 MB/s (-60%)    |
| 4          | connect       |           64 |            4 |          44 |           76 |        50,000 |                    |
| 4          | kafka_connect |           17 |           17 |          17 |           17 |             0 | -47 MB/s (-74%)    |
| 8          | connect       |           92 |            6 |          51 |          102 |        72,794 |                    |
| 8          | kafka_connect |           17 |           17 |          15 |           17 |             0 | -75 MB/s (-82%)    |


### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
| 2    | connect       | kafka_connect | 2.53x |          95 |          38 |
| 4    | connect       | kafka_connect | 3.80x |          64 |          17 |
| 8    | connect       | kafka_connect | 5.45x |          92 |          17 |

Raw samples + Prometheus snapshots: [`results/mysql/orders-cdc/2026-06-01T17-04-23Z.json`](results/mysql/orders-cdc/2026-06-01T17-04-23Z.json)


## AWS — orders-cdc-vcpu4 — 2026-06-01

**Scenario:** Single-point diagnostic variant of mysql-orders-cdc, pinned to vCPU=4 only.
Used 2026-06-01 to test whether KC's vCPU=4 plateau at 17 MB/s lifts when
the producer moves from db.r6g.2xlarge to db.r6g.4xlarge.

Run with: `task aws:bench scenario=mysql/orders-cdc-vcpu4`
Expected wall-clock: ~55-60 min. Expected spend: ~$3-4.

Delete this file after the experiment is done.

**Git SHA:** [`44ab9e9f7`](https://github.com/redpanda-data/connect/commit/44ab9e9f7a64cb53b2a1d27f5cbbb2b305a61d15)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.4xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 4          | connect       |          111 |            7 |         107 |          114 |        87,441 |                    |
| 4          | kafka_connect |           51 |           51 |          39 |           54 |             0 | -60 MB/s (-54%)    |


### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
| 4    | connect       | kafka_connect | 2.16x |         111 |          51 |

Raw samples + Prometheus snapshots: [`results/mysql/orders-cdc-vcpu4/2026-06-01T19-52-07Z.json`](results/mysql/orders-cdc-vcpu4/2026-06-01T19-52-07Z.json)
