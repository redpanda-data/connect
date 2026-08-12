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
>    grant permission to do more work per cycle. NOTE (2026-08-11, RDS
>    CloudWatch pulled post-sweep — metrics outlive the instance): the DB was
>    STORAGE-THROUGHPUT saturated during every window. WriteThroughput pinned
>    at ~195-197 MB/s (~10x write amplification over the ~20 MB/s of logical
>    rows: base table + txn log + change table + checkpoints), CPU <= 50%,
>    DiskQueueDepth spiking to 249. The capture job's log scanner shares that
>    volume, so "the single-threaded scan is the intrinsic limit" is NOT
>    established — the scanner was plausibly IO-starved. The rds-mssql module
>    provisions gp3 IOPS (24000) but never sets storage_throughput; that is
>    the prime suspect and the cheap A/B. Until it runs, every ABSOLUTE
>    rows/sec figure below is conditional on ~195 MB/s of storage throughput.
>    The RELATIVE Connect-vs-Debezium comparison and the 2 vCPU knee are
>    unaffected — both engines faced the same saturated volume.
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
> - **The 4-point sweep exists (collated table below)** and the per-core curve
>   is NOT flat, contrary to an earlier prediction here: burst-drain capacity
>   scales with cores even though the capture job caps the mean.
>
> What the data does support: `microsoft_sql_server_cdc` sustains ~34K records/sec
> at p95 on a single vCPU, roughly 1.8x Debezium's peak, and the practical limit
> for SQL Server CDC is the capture job rather than the connector.

## Storage-throughput A/B (2026-08-12): storage was the ceiling, measured

One arm, 2 vCPU Connect-only, identical to the sweep's 2 vCPU point except
gp3 `storage_throughput` provisioned at 1000 MiB/s (the sweep ran at the RDS
default). Fresh infra, same instance class.

| 2 vCPU Connect | sweep (default gp3) | A/B (1000 MiB/s) | Δ |
|---|---|---|---|
| offered load (rows/s) | ~16,400 | 25,440 | +55% |
| delivered mean rec/s | 16,215 | 22,898 | +41% |
| duty cycle | 82% | 92% | +10 pts |
| CloudWatch WriteThroughput | pinned ~195 MB/s | peak ~313 MB/s | +60% |
| CloudWatch DiskQueueDepth | to 249 | ~3 | — |

Reading:

- **Provisioning gp3 throughput moved everything** — the write path (+55%
  offered) and the capture job's publication rate (+41% delivered). The
  earlier claim that the capture job's single-threaded log scan was an
  intrinsic ceiling is REFUTED: it was IO-starved, not CPU-limited, in every
  sweep window.
- **The new ceiling is the instance's EBS bandwidth**: 313 MB/s is the
  db.r5.2xlarge EBS baseline almost exactly, with the disk queue now shallow.
  Neither arm reached the capture job's intrinsic limit.
- **Sizing rule of thumb this yields:** with ~10x write amplification (base
  table + txn log + change table + checkpoints), SQL Server CDC's logical
  change throughput on RDS is roughly storage-bandwidth / 10. Buy the
  instance's EBS bandwidth and gp3 throughput accordingly; Connect needs only
  2 vCPU to track whatever that provides.
- The sweep table below is the DEFAULT-gp3 curve. Its relative claims stand
  (both engines shared each volume); its absolute numbers are that
  configuration's, not SQL Server's.

## Collated 4-point sweep (2026-08-10..11, bulk-copy load, 500ms backoff)

One 15-minute window per engine per point; offered load ~16.7-19.0M rows per
window where captured (`[groundtruth]`). Records/sec, broker-derived, mean over
the window — see the caveat above for why median and MB/s are not the right
columns here.

| vCPU | Connect mean rec/s | Connect p95 rec/s | Connect duty | Connect peak MB/s | Debezium mean rec/s | Debezium duty |
|------|-----|-----|-----|-----|-----|-----|
| 1 | 10,056 | 33,586 | 44% | 42.7 | 9,556¹ | 100% |
| 2 | 16,215 | 38,852 | 82% | 62.8 | 10,703 | 100% |
| 4 | 16,943 | 41,476 | 86% | 89.7 | 7,037² | 68% |
| 8 | 10,431³ | 30,365 | 51% | 108.6 | 12,261 | 100% |

¹ From the 1s-backoff run (101 broker samples); the 500ms run's Debezium series
truncated to 11 samples and is unusable.
² Real offered load (17.4M rows) — not a weak write window — but single-run.
³ Offered load was the HIGHEST of any window (19.0M rows), so not load-starved;
single-run, needs a repeat before quoting as an 8-core degradation.

**Reading of the curve:**

- **Connect's burst-drain capacity scales near-linearly with cores** (peak 42.7
  → 62.8 → 89.7 → 108.6 MB/s), but mean throughput is capture-job-bound with a
  **knee at 2 vCPU**: +61% from 1→2, +4% from 2→4. At 2-4 vCPU Connect converts
  ~94% of offered load; the constraint above that is SQL Server publishing
  changes, not the connector reading them.
- **Debezium runs at ~100% duty cycle at every core count but its mean never
  leaves the 7-12K band** — steady, lower ceiling, and no clear gain from cores.
- **Sizing answer: 2 vCPU is the sweet spot for `microsoft_sql_server_cdc`**;
  at that point it leads Debezium by ~52% on delivered records against the same
  load. Point-to-point comparisons beyond that are variance-dominated because
  offered load swings 14-24% between windows — compare capture ratios, not raw
  means.




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


## AWS — orders-cdc — 2026-08-11

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

**Git SHA:** [`6cfc6928e`](https://github.com/redpanda-data/connect/commit/6cfc6928eb2c43592c1a71b2f864a7ad36bc10fd)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 2    | 2          |                | connect       |           20 |       19.780 |        16,215 |           21 |           0 |           47 |        16,600 |                    |


> ⚠ At 2 vCPU: 64s dip to 0.00× median MB/sec from t=455s — investigate before publishing.



Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-11T15-36-19Z.json`](results/sqlserver/orders-cdc/2026-08-11T15-36-19Z.json)


## AWS — orders-cdc — 2026-08-11

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

**Git SHA:** [`f153b9220`](https://github.com/redpanda-data/connect/commit/f153b922022ebec31971538ff7c6424b1c81ec8c)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 2    | 2          |                | kafka_connect |           18 |       19.168 |        10,702 |           19 |           2 |           40 |        10,128 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-11T15-57-37Z.json`](results/sqlserver/orders-cdc/2026-08-11T15-57-37Z.json)


## AWS — orders-cdc — 2026-08-11

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

**Git SHA:** [`f153b9220`](https://github.com/redpanda-data/connect/commit/f153b922022ebec31971538ff7c6424b1c81ec8c)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 4    | 4          |                | connect       |           20 |       20.667 |        16,942 |           20 |           0 |           51 |        16,222 |                    |
| 4    | 4          |                | kafka_connect |            4 |       12.600 |         7,036 |            4 |           0 |           44 |         1,997 | -14,225 msg/s (-88%) |


> ⚠ At 4 vCPU: 93s dip to 0.00× median MB/sec from t=762s — investigate before publishing.



### Cross-engine divergence

| vCPU | faster        | slower        | ratio  | faster MB/s | slower MB/s |
|------|---------------|---------------|--------|-------------|-------------|
| 4    | connect       | kafka_connect | 5.53x |          20 |           4 |

Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-11T16-21-43Z.json`](results/sqlserver/orders-cdc/2026-08-11T16-21-43Z.json)


## AWS — orders-cdc — 2026-08-11

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

**Git SHA:** [`c0dba5963`](https://github.com/redpanda-data/connect/commit/c0dba5963f73d145da2cfd23f9671803ec834628)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 8    | 8          |                | connect       |            4 |       12.724 |        10,430 |            4 |           0 |           37 |         3,058 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-11T17-24-21Z.json`](results/sqlserver/orders-cdc/2026-08-11T17-24-21Z.json)


## AWS — orders-cdc — 2026-08-11

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

**Git SHA:** [`c0dba5963`](https://github.com/redpanda-data/connect/commit/c0dba5963f73d145da2cfd23f9671803ec834628)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 8    | 8          |                | kafka_connect |           26 |       21.961 |        12,261 |           26 |           3 |           41 |        14,345 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-11T18-02-38Z.json`](results/sqlserver/orders-cdc/2026-08-11T18-02-38Z.json)


## AWS — orders-cdc — 2026-08-12

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

**Git SHA:** [`9a647085f`](https://github.com/redpanda-data/connect/commit/9a647085f68c45fb2e7c1a169a9b1e5e57f205c8)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 2    | 2          |                | connect       |           33 |       27.932 |        22,898 |           33 |           0 |           45 |        27,000 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-12T00-42-11Z.json`](results/sqlserver/orders-cdc/2026-08-12T00-42-11Z.json)


## AWS — orders-cdc — 2026-08-12

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

**Git SHA:** [`8463f8e12`](https://github.com/redpanda-data/connect/commit/8463f8e121b30447a87355e8017237dbc46f3eb2)

**Infra:** Runner `c8g.4xlarge`; source `db.r5.2xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 2    | 2          |                | kafka_connect |           41 |       27.211 |        15,188 |           41 |           0 |           52 |        22,780 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-12T01-50-23Z.json`](results/sqlserver/orders-cdc/2026-08-12T01-50-23Z.json)


## AWS — orders-cdc — 2026-08-12

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

**Git SHA:** [`8463f8e12`](https://github.com/redpanda-data/connect/commit/8463f8e121b30447a87355e8017237dbc46f3eb2)

**Infra:** Runner `c8g.4xlarge`; source `db.r5b.4xlarge` (800 GB) in `us-east-2`.

**Dataset:** 

### Throughput

| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
| 2    | 2          |                | connect       |           46 |       40.621 |        33,301 |           46 |           0 |           63 |        37,850 |                    |


Raw samples + Prometheus snapshots: [`results/sqlserver/orders-cdc/2026-08-12T02-59-41Z.json`](results/sqlserver/orders-cdc/2026-08-12T02-59-41Z.json)

## Follow-up runs (2026-08-12): Debezium measured at provisioned throughput, and the capture job is still not the ceiling

Two single-point runs (2 vCPU, the knee) close the two gaps the storage A/B
left open. Raw sections above (`01-50-23Z` = Debezium, `02-59-41Z` = Connect
big-EBS); analysis here.

### 1. Debezium at gp3 1000 MiB/s — the head-to-head at the provisioned config is now measured, not extrapolated

| 2 vCPU | offered (rows/s) | delivered mean (rec/s) | capture ratio | p95 rec/s |
|---|--:|--:|--:|--:|
| Connect (A/B run, `00-42-11Z`) | 25,440 | 22,898 | ~90% | — |
| Debezium (`01-50-23Z`) | 33,542 | 15,189 | ~45% | 29,243 |

Connect **+51% on delivered mean** — the sweep's +52% figure survives the
storage fix almost unchanged — and roughly **2× on capture ratio**, the
offered-load-independent comparison. Debezium's window was not
storage-starved: CloudWatch WriteThroughput avg 240 MB/s (max 374, burst above
the r5.2xlarge 287.5 baseline), DiskQueueDepth avg 4.9, CPU ≤ 53%. Its
deficit is duty cycle, same shape as before.

### 2. db.r5b.4xlarge + gp3 2000 MiB/s — storage exonerated, capture job still not the wall

Connect, 2 vCPU, source EBS baseline lifted 287.5 → 1250 MB/s (~4×):

- Delivered **33,302 rec/s mean** (median 37,850, p95 51,791, **peak 63,000**)
  of 39,137 rows/s offered — **~85% capture, +45%** over the same Connect
  config on db.r5.2xlarge.
- CloudWatch: WriteThroughput avg 282 / max 430 MB/s of ~1250 available
  (**23% utilization**), DiskQueueDepth avg 3.0 (vs 249 when storage-bound),
  DB CPU 24%. Storage and DB CPU are both loafing.
- The binding constraint is now the **offered load**: the c8g.large load
  generator plateaued at ~39K rows/s mean (55K peak) of its 150K target with
  the DB no longer pushing back. The capture job's intrinsic ceiling is STILL
  unmeasured — but it is now bounded below at **≥33K rec/s sustained / ≥63K
  burst**, 2× the full sweep's best point. A bigger load-gen instance is the
  next lever, not a bigger DB.
- Write amplification at this tier measured ~6× (282 MB/s physical for
  ~47 MB/s logical), gentler than the ~10× observed at the saturated tier —
  the storage-bandwidth/10 sizing rule is conservative, which is the right
  direction for a customer-facing rule.

Ceiling chain after these runs: load-gen → capture job (unreached) → storage
(only if unprovisioned). Neither engine's connector is the limit at 2 vCPU.
