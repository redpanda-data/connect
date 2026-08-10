<!-- Copyright 2025 Redpanda Data, Inc. -->
# SQL Server (microsoft_sql_server_cdc)

> **READ THIS BEFORE QUOTING ANY NUMBER BELOW. The constraint here is SQL
> Server's CDC capture job, which sits upstream of BOTH engines. These are not
> connector ceilings.**
>
> Three ceilings were found in sequence, each hidden behind the previous one. The
> first two were bench defects and are fixed; the third is a property of SQL
> Server:
>
> 1. **Identical row payloads** made every producer batch compress ~14x, so
>    byte-based metrics measured compression, not throughput. Fixed (distinct
>    payload pool).
> 2. **Parameterized multi-row INSERT** capped the load generator at ~11K
>    rows/sec regardless of batch size, parameter type, or load-gen instance size
>    (2 vs 16 vCPU made no difference). Fixed with TDS bulk copy: ~2.3x more load,
>    ~18-20M rows per point.
> 3. **The CDC capture job cannot keep pace with that load.** After every
>    ~18-20M-row window, `sys.fn_cdc_get_max_lsn()` stays frozen for 4+ minutes
>    while the job catches up. It publishes in bursts, so readers alternate
>    between draining at ~42 MB/s and sitting idle. `@maxtrans`/`@maxscans`/
>    `@pollinginterval` are already tuned far above what the load needs - they
>    grant permission to do more work per cycle, they do not make the
>    single-threaded log scan faster.
>
> Consequences for reading the tables below:
>
> - **IGNORE the `MB/sec (p50)` column for Connect.** On a bimodal burst/idle
>   series the median reads 0.00 while p95 is ~41 and peak ~42. Use mean and the
>   `msg/sec` columns.
> - **The `Δ vs Connect` column is records-based** (as of the records-metric fix)
>   and so is comparable; the MB/s columns are not comparable between engines,
>   because Debezium's JSON envelope is ~60% fatter per record.
> - **Offered load varies 14-24% between points**, so compare capture ratio
>   (records delivered / rows committed, from the `[groundtruth]` log line) rather
>   than raw throughput. At 1 vCPU Connect captured ~43% of offered load with a 1s
>   backoff and ~50% with 500ms.
> - **Two runs have unusable Debezium percentiles**, from broker scrapes that
>   truncated to 11 and 13 samples versus the normal ~100. Check
>   `broker_series` length before quoting a KC spread.
> - **Two early runs are broken, not results**: one where a stopped capture job
>   reported 0 (fixed by `ensureCaptureJobRunning`), and the pre-bulk-copy runs
>   that were load-bound at ~11.5 MB/s.
> - **No multi-vCPU sweep exists.** Only 1 vCPU has been measured. A per-core
>   curve would be flat for reasons upstream of Connect, so it was not run.
>
> What the data does support: `microsoft_sql_server_cdc` sustains ~34K records/sec
> at p95 on a single vCPU, roughly 1.8x Debezium's peak, and the practical limit
> for SQL Server CDC is the capture job rather than the connector.



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


## AWS — orders-cdc — 2026-08-10

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

**Git SHA:** [`f000beab3`](https://github.com/redpanda-data/connect/commit/f000beab308e204158eac42afc5bafa07ac9f8f9)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |            0 |        9.280 |         7,607 |            0 |           0 |           42 |             0 |                    |
| 1    | 1          |                | kafka_connect |           19 |       17.113 |         9,556 |           19 |           2 |           34 |        10,831 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-10T18-18-37Z.json`](results/sqlserver/orders-cdc/2026-08-10T18-18-37Z.json)


## AWS — orders-cdc — 2026-08-10

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

**Git SHA:** [`f000beab3`](https://github.com/redpanda-data/connect/commit/f000beab308e204158eac42afc5bafa07ac9f8f9)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 1    | 1          |                | connect       |            0 |       12.267 |        10,056 |            0 |           0 |           41 |             0 |                    |
| 1    | 1          |                | kafka_connect |           16 |       14.686 |         8,206 |           16 |           1 |           17 |         9,177 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-10T20-20-56Z.json`](results/sqlserver/orders-cdc/2026-08-10T20-20-56Z.json)
