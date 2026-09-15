# Snowflake Benchmark Suite

Measures write throughput for both Snowflake output connectors, and (optionally)
the official Snowflake Kafka Connector as a comparison baseline.

## What each folder measures

| Folder             | Connector             | Mechanism                 | Notes                                             |
| ------------------ | --------------------- | ------------------------- | ------------------------------------------------- |
| `write/streaming/` | `snowflake_streaming` | Snowpipe Streaming API    | Low-latency, strongly-typed, no warehouse needed  |
| `write/bulk/`      | `snowflake_put`       | Stage file PUT + Snowpipe | High-throughput batch uploads; warehouse required |

There is no Redpanda Connect Snowflake input connector — read-side benchmarks are out of scope.

Each folder has two bench flavors:
- **`bench:*` tasks** — Redpanda Connect (native connector), produces records in-process, no Kafka involved
- **`bench:kc:*` tasks** — [official Snowflake Kafka Connector](https://docs.snowflake.com/en/user-guide/kafka-connector/index), reads from a local Kafka topic (`bench:load` fills it first)

`write/bulk`'s `bench:kc:*` runs the connector in classic Snowpipe mode (comparable to `snowflake_put`); `write/streaming`'s runs it in Snowpipe Streaming mode (comparable to `snowflake_streaming`). Both land in their own `_KC`-suffixed table (connector-managed `RECORD_METADATA`/`RECORD_CONTENT` VARIANT columns, schematization off) so they never collide with the native bench's tables.

For deploying either flavor to an EC2 box instead of running locally, see [`deploy/snowflake-aws/`](../../../../deploy/snowflake-aws/).

---

## Prerequisites

### Both flavors
- Snowflake account with a user that has the required privileges
- RSA key pair configured for the user ([docs](https://docs.snowflake.com/en/user-guide/key-pair-auth))
- `snowsql` in PATH (for `setup` / `teardown` tasks)
- Go toolchain (for running pipelines via `go run`)

### Kafka Connect only
- Docker running (`task up` starts a local single-node Kafka; `task kc:build`/`task kc:up` build and start Kafka Connect with the connector jar pulled from Maven Central)

---

## Credentials

All tasks read credentials from environment variables:

| Variable                | Required               | Description                                 |
| ----------------------- | ----------------------- | ------------------------------------------- |
| `SNOWFLAKE_ACCOUNT`     | yes                     | Account identifier (e.g. `MYORG-MYACCOUNT`) |
| `SNOWFLAKE_USER`        | yes                     | Snowflake user name                         |
| `SNOWFLAKE_DB`          | yes                     | Target database                             |
| `SNOWFLAKE_PRIVATE_KEY` | yes                     | PEM-encoded RSA private key content         |
| `SNOWFLAKE_WAREHOUSE`   | write-bulk (native) only | Warehouse name                              |
| `SNOWFLAKE_ROLE`        | streaming `bench:kc:*` only | Snowpipe Streaming KC mode requires it; optional elsewhere, default: unset (user's default role) |
| `SNOWFLAKE_SCHEMA`      | no                      | Default: `RAW`                              |

---

## Setup

Run once before any benchmark:

```bash
task setup
```

This creates:
- `BENCH_EVENTS` — typed table for `write/streaming`
- `BENCH_EVENTS_JSON` — VARIANT table for `write/bulk`
- `BENCH_STAGE` — internal stage for PUT uploads
- `BENCH_PIPE` — Snowpipe wired to `BENCH_STAGE` → `BENCH_EVENTS_JSON`

`BENCH_EVENTS_KC` / `BENCH_EVENTS_JSON_KC` (the `bench:kc:*` target tables) aren't part of `create.sql` — the Kafka connector auto-creates them on first connector start. `task teardown` drops them along with everything else.

---

## Run benchmarks

```bash
# Snowpipe Streaming (write-streaming)
task bench:streaming                              # defaults
task bench:streaming BATCH=5000 PARALLELISM=4    # tuned

# Staged PUT + Snowpipe (write-bulk)
task bench:bulk                                   # defaults
task bench:bulk BATCH=5000 UPLOAD_THREADS=8      # tuned
```

Or run directly from each subfolder for the parameter matrix:

```bash
cd write/streaming && task bench:matrix        # one dimension at a time
cd write/streaming && task bench:matrix:full   # full BATCH x PARALLELISM x MAX_IN_FLIGHT x CHUNK_SIZE cross product
cd write/bulk      && task bench:matrix        # full BATCH x MAX_IN_FLIGHT cross product
```

`bench:matrix:full` defaults to a small grid (3 batches x 2 parallelism x 2 max_in_flight x 1 chunk_size = 12 runs) — override `BATCHES`/`PARALLELS`/`MAX_IN_FLIGHTS`/`CHUNK_SIZES` to widen it. The full cross product of `bench:matrix`'s default lists is 7x8x7x7 = 2,744 runs, impractical against live Snowflake.

---

## Quick start — Kafka Connect comparison

Run from `write/bulk/` or `write/streaming/` — each folder's Kafka + Kafka Connect stack is self-contained (unlike the SAP HANA bench, there's no shared broker between folders since native runs don't need Kafka at all).

```bash
cd write/bulk   # or write/streaming

export SNOWFLAKE_ACCOUNT="MYORG-MYACCOUNT"
export SNOWFLAKE_USER="bench_user"
export SNOWFLAKE_DB="BENCH_DB"
export SNOWFLAKE_PRIVATE_KEY="$(cat /path/to/key.p8)"
export SNOWFLAKE_ROLE="BENCH_ROLE"   # required for write/streaming's bench:kc:run

task up                              # start local Kafka
task kc:build                        # downloads the connector jar from Maven Central (needs internet)
task kc:up                           # start Kafka Connect

task bench:load COUNT=1000000        # produce test messages into the local topic
task bench:kc:run                    # single run
task bench:kc:matrix OUT=kc.txt      # sweep buffer_count x tasks.max

task down
```

### Parameter mapping: Redpanda Connect vs Kafka Connect

| Dimension          | Redpanda Connect (`write/bulk`) | Redpanda Connect (`write/streaming`) | Kafka Connect                        |
| ------------------- | -------------------------------- | -------------------------------------- | ------------------------------------- |
| Write batch size    | `BATCH` (`batching.count`)       | `BATCH` (`batching.count`)             | `BUFFER_COUNT` (`buffer.count.records`) |
| Flush interval      | `PERIOD` (`batching.period`)     | `PERIOD` (`batching.period`)           | `BUFFER_FLUSH_SEC` (bulk only; `buffer.flush.time`) |
| Streaming flush lag | n/a                               | n/a                                     | `MAX_CLIENT_LAG` (streaming only; `snowflake.streaming.max.client.lag`) |
| Parallelism         | `UPLOAD_THREADS` / `MAX_IN_FLIGHT` | `PARALLELISM` / `MAX_IN_FLIGHT`      | `TASKS` (`tasks.max`)                 |

---

## Teardown

```bash
task teardown
```
