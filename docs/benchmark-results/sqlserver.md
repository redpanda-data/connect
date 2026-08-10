<!-- Copyright 2025 Redpanda Data, Inc. -->
# SQL Server (microsoft_sql_server_cdc)

> **READ THIS BEFORE QUOTING ANY NUMBER BELOW. These runs are LOAD-BOUND smokes,
> not a benchmark result.**
>
> Every run recorded here delivered only ~10-11K rows/sec against the scenario's
> 150,000 rows/sec target — roughly 7% of ask, about 11.5 MB/s. Neither engine was
> the constraint: both tracked the offered load almost exactly. The write path
> caps server-side in SQL Server (~3s per 1000-row insert regardless of load-gen
> instance size, with periodic stalls to ~3K rows/sec). Until that is lifted, these
> figures measure the load generator, not `microsoft_sql_server_cdc`.
>
> Specific traps in the tables below:
>
> - **The `Δ vs Connect` column is a BYTE comparison and does not mean Connect is
>   slower.** At near-identical record rates (9,400 vs 10,319 records/sec, within
>   10%) Debezium ships ~60% more bytes because its JSON envelope is fatter. Read
>   the `msg/sec` columns, which are compression- and envelope-independent.
> - **The first run below is broken**, not a result: `kafka_connect` reads 0
>   because the reset left SQL Server's CDC capture job stopped. Fixed by
>   `ensureCaptureJobRunning` in `seeders/cdc-rows-mssql/sql.go`.
> - No multi-vCPU sweep has been run, so there is no scaling curve here.



## AWS — orders-cdc — 2026-08-07

**Scenario:** Stream changes from a high-write SQL Server orders table so the
microsoft_sql_server_cdc input — not the producer, and not SQL Server's own
capture job — is the bottleneck across the CPU sweep.

Fairness: neither engine reads the transaction log. Connect's
microsoft_sql_server_cdc and the Kafka Connect comparator (Debezium SQL
Server) both tail the same cdc.<schema>_<table>_CT change tables, populated by
SQL Server's capture job. That makes this the most apples-to-apples CDC
head-to-head in the suite — and it also means the capture job sits upstream of
BOTH engines. The seeder raises its per-cycle limits well past what the load
generator can produce (see tuneCaptureJob in seeders/cdc-rows-mssql/sql.go);
leave the stock 10/500/5 in place and you measure the capture job's ~1000
transactions/sec ceiling and mislabel it a connector ceiling.

**Git SHA:** [`6d2e7e318`](https://github.com/redpanda-data/connect/commit/6d2e7e3189b823f8893d5eb19f7a69622c71bf3e)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |           10 |       12.066 |         9,516 |            1 |           0 |           32 |         8,000 |                    |
| 1    | 1          |                | kafka_connect |            0 |        0.000 |             0 |            0 |           0 |            0 |             0 | -10 MB/s (-100%)   |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-07T01-45-22Z.json`](results/sqlserver/orders-cdc/2026-08-07T01-45-22Z.json)


## AWS — orders-cdc — 2026-08-07

**Scenario:** Stream changes from a high-write SQL Server orders table so the
microsoft_sql_server_cdc input — not the producer, and not SQL Server's own
capture job — is the bottleneck across the CPU sweep.

Fairness: neither engine reads the transaction log. Connect's
microsoft_sql_server_cdc and the Kafka Connect comparator (Debezium SQL
Server) both tail the same cdc.<schema>_<table>_CT change tables, populated by
SQL Server's capture job. That makes this the most apples-to-apples CDC
head-to-head in the suite — and it also means the capture job sits upstream of
BOTH engines. The seeder raises its per-cycle limits well past what the load
generator can produce (see tuneCaptureJob in seeders/cdc-rows-mssql/sql.go);
leave the stock 10/500/5 in place and you measure the capture job's ~1000
transactions/sec ceiling and mislabel it a connector ceiling.

**Git SHA:** [`d96504aec`](https://github.com/redpanda-data/connect/commit/d96504aec339bd0452651bd698c196ab8e2f7b55)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |           11 |       10.941 |         8,610 |           12 |           0 |           25 |         9,000 |                    |
| 1    | 1          |                | kafka_connect |           17 |       14.996 |         8,379 |           17 |           1 |           17 |         9,519 | +6 MB/s (+55%)     |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-07T04-34-38Z.json`](results/sqlserver/orders-cdc/2026-08-07T04-34-38Z.json)


## AWS — orders-cdc — 2026-08-07

**Scenario:** Stream changes from a high-write SQL Server orders table so the
microsoft_sql_server_cdc input — not the producer, and not SQL Server's own
capture job — is the bottleneck across the CPU sweep.

Fairness: neither engine reads the transaction log. Connect's
microsoft_sql_server_cdc and the Kafka Connect comparator (Debezium SQL
Server) both tail the same cdc.<schema>_<table>_CT change tables, populated by
SQL Server's capture job. That makes this the most apples-to-apples CDC
head-to-head in the suite — and it also means the capture job sits upstream of
BOTH engines. The seeder raises its per-cycle limits well past what the load
generator can produce (see tuneCaptureJob in seeders/cdc-rows-mssql/sql.go);
leave the stock 10/500/5 in place and you measure the capture job's ~1000
transactions/sec ceiling and mislabel it a connector ceiling.

**Git SHA:** [`d96504aec`](https://github.com/redpanda-data/connect/commit/d96504aec339bd0452651bd698c196ab8e2f7b55)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |           11 |       10.166 |         8,332 |           11 |           2 |           14 |         9,400 |                    |
| 1    | 1          |                | kafka_connect |           18 |       16.271 |         9,086 |           18 |           1 |           21 |        10,319 | +7 MB/s (+61%)     |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-07T18-00-43Z.json`](results/sqlserver/orders-cdc/2026-08-07T18-00-43Z.json)
