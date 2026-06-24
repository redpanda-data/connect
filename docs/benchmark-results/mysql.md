# MySQL Benchmark Results

AWS bench results for `mysql_cdc`. Per-scenario sections are appended by `task aws:bench` (see `benchmarking/aws/README.md`). For local laptop-Docker mysql results see [`mysql-cdc.md`](./mysql-cdc.md).

---

## AWS — orders-cdc — 2026-06-02

**Scenario:** Stream changes from a high-write MySQL orders table (target 150K writes/sec
≈ 180 MB/s) so the mysql_cdc input — not the producer — is the bottleneck
across the whole CPU sweep. TRUNCATE between sweep points keeps the table
size bounded (no Trap 3).

**Git SHA:** [`25057d693`](https://github.com/redpanda-data/connect/commit/25057d6936c7785ca918aa09eac8a1341afcf875)

**Infra:** Runner `c8g.4xlarge`; source `db.r6g.4xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| GOMAXPROCS | engine        | MB/sec (p50) | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------------|---------------|--------------|-------------|-------------|--------------|---------------|--------------------|
| 1          | connect       |           70 |            4 |          64 |           75 |        55,000 |                    |
| 1          | kafka_connect |           33 |           33 |          31 |           35 |             0 | -37 MB/s (-53%)    |
| 2          | connect       |          102 |            6 |          95 |          108 |        80,000 |                    |
| 2          | kafka_connect |           35 |           35 |          32 |           36 |             0 | -67 MB/s (-65%)    |
| 4          | connect       |          108 |            6 |         104 |          114 |        85,000 |                    |
| 4          | kafka_connect |           54 |           54 |          50 |           54 |             0 | -54 MB/s (-50%)    |
| 8          | connect       |          111 |            7 |         105 |          114 |        87,167 |                    |
| 8          | kafka_connect |           39 |           39 |          34 |           43 |             0 | -72 MB/s (-65%)    |


### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
| 1    | connect       | kafka_connect | 2.14x |          70 |          33 |
| 2    | connect       | kafka_connect | 2.90x |         102 |          35 |
| 4    | connect       | kafka_connect | 2.02x |         108 |          54 |
| 8    | connect       | kafka_connect | 2.88x |         111 |          39 |

Raw samples + Prometheus snapshots: [`results/mysql/orders-cdc/2026-06-02T14-13-52Z.json`](results/mysql/orders-cdc/2026-06-02T14-13-52Z.json)
