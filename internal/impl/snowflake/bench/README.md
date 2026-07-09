# Snowflake Benchmark Suite

Measures write throughput for both Snowflake output connectors.

## What each folder measures

| Folder | Connector | Mechanism | Notes |
|---|---|---|---|
| `write/streaming/` | `snowflake_streaming` | Snowpipe Streaming API | Low-latency, strongly-typed, no warehouse needed |
| `write/bulk/` | `snowflake_put` | Stage file PUT + Snowpipe | High-throughput batch uploads; warehouse required |

There is no Redpanda Connect Snowflake input connector — read-side benchmarks are out of scope.

---

## Prerequisites

- Snowflake account with a user that has the required privileges
- RSA key pair configured for the user ([docs](https://docs.snowflake.com/en/user-guide/key-pair-auth))
- `snowsql` in PATH (for `setup` / `teardown` tasks)
- Go toolchain (for running pipelines via `go run`)

---

## Credentials

All tasks read credentials from environment variables:

| Variable | Required | Description |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | yes | Account identifier (e.g. `MYORG-MYACCOUNT`) |
| `SNOWFLAKE_USER` | yes | Snowflake user name |
| `SNOWFLAKE_DB` | yes | Target database |
| `SNOWFLAKE_PRIVATE_KEY` | yes | PEM-encoded RSA private key content |
| `SNOWFLAKE_WAREHOUSE` | write-bulk only | Warehouse name |
| `SNOWFLAKE_ROLE` | no | Default: `ACCOUNTADMIN` |
| `SNOWFLAKE_SCHEMA` | no | Default: `PUBLIC` |

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

Or run directly from each subfolder for the full parameter matrix:

```bash
cd write/streaming && task bench:matrix
cd write/bulk      && task bench:matrix
```

---

## Teardown

```bash
task teardown
```
