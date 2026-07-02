# SAP HANA Benchmark Suite

Benchmarks SAP HANA throughput using the `sap_hana` connector (Redpanda Connect) and Kafka Connect JDBC connector side by side.

## What each folder measures

| Folder | Mode | Description |
|---|---|---|
| `saphana-read/bulk/` | `bulk` | Read all rows once → Kafka |
| `saphana-read/incrementing/` | `incrementing` | Poll for net-new rows using HWM column → Kafka |
| `saphana-read/query/` | `query` | Execute user-supplied SQL → Kafka |
| `saphana-write/` | `sql_insert` | Kafka → SAP HANA table |

Each folder has two bench flavors:
- **`bench:*` tasks** — Redpanda Connect (`sap_hana` connector)
- **`bench:kc:*` tasks** — Kafka Connect JDBC connector

---

## Prerequisites

### Both flavors
- Docker running
- `HANA_DSN` env var: `export HANA_DSN="hdb://USER:PASS@host:30015"`
- `HANA_SCHEMA` env var: `export HANA_SCHEMA="MY_SCHEMA"`

### Kafka Connect only — `ngdbc.jar`

The JDBC connector needs the SAP HANA JDBC driver. Place it at `bench/ngdbc.jar` (one level above each bench folder):

```
bench/
  ngdbc.jar        ← place here
  saphana-read/
  saphana-write/
```

**Where to get `ngdbc.jar`:**
- On the HANA server: `find / -name "ngdbc.jar" 2>/dev/null`
- SAP Support Portal: [support.sap.com](https://support.sap.com) → SAP HANA Client download
- HANA client install directory: typically `<hana_client>/lib/ngdbc.jar`

---

## Quick start — Redpanda Connect

```bash
cd saphana-read/bulk

export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="MY_SCHEMA"

task up                          # start Kafka
task bench:build                 # build binaries
task bench:load COUNT=2000000    # create BENCH_ORDERS table + load rows
task bench:run                   # single run
task bench:matrix OUT=rpcn.txt   # full sweep: fetch x batch x cores
```

---

## Quick start — Kafka Connect

> Run these from `saphana-read/bulk/` — Kafka Connect is shared by all 4 bench folders.

```bash
cd saphana-read/bulk

# 1. Place ngdbc.jar at bench/ngdbc.jar (see Prerequisites above)

# 2. Build the Kafka Connect docker image (once — downloads ~50 MB JDBC plugin)
task kc:build

# 3. Start Kafka (if not already running)
task up

# 4. Start Kafka Connect
task kc:up

# 5. Run KC bench in the relevant folder
task bench:kc:matrix TOTAL=2000000 OUT=kc.txt
```

KC uses the same Kafka container (`rpcns-hana-kafka`) as the Redpanda Connect benches, so `task up` only needs to run once for both flavors.

---

## Parameter mapping: Redpanda Connect vs Kafka Connect

| Dimension | Redpanda Connect | Kafka Connect |
|---|---|---|
| DB fetch batch | `fetch_size` | `jdbc.fetch.size` |
| Kafka produce batch | `batching.count` | `batch.max.rows` |
| Poll interval (inc.) | `poll_interval` | `poll.interval.ms` |
| HANA parallelism | `GOMAXPROCS` (cores) | n/a (single-task per query) |
| Write batch (write) | `max_in_flight` | `batch.size` |
| Write parallelism | `GOMAXPROCS` | `tasks.max` |

---

## Bench matrices at a glance

### Bulk read

| Flavor | Parameters swept | Runs |
|---|---|---|
| rpcn | `fetch` × `batch` × `cores` (3×3×4) | 36 |
| KC | `fetch` × `batch_max_rows` (3×3) | 9 |

```bash
# in saphana-read/bulk/
task bench:matrix OUT=rpcn_bulk.txt
task bench:kc:matrix TOTAL=2000000 OUT=kc_bulk.txt
```

### Incrementing read

| Flavor | Parameters swept | Runs |
|---|---|---|
| rpcn | `fetch` × `poll` × `cores` (3×3×4) | 36 |
| KC | `fetch` × `poll` (3×3) | 9 |

```bash
# in saphana-read/incrementing/
task bench:matrix COUNT=500000 OUT=rpcn_inc.txt
task bench:kc:matrix COUNT=500000 OUT=kc_inc.txt
```

### Query read

| Flavor | Parameters swept | Runs |
|---|---|---|
| rpcn | `fetch` × `cores` (3×4) | 12 |
| KC | `fetch` × `batch_max_rows` (3×3) | 9 |

```bash
# in saphana-read/query/
task bench:matrix OUT=rpcn_query.txt
task bench:kc:matrix TOTAL=2000000 OUT=kc_query.txt
```

### Write

| Flavor | Parameters swept | Runs |
|---|---|---|
| rpcn | `max_in_flight` × `cores` (4×4) | 16 |
| KC | `batch_size` × `tasks` (4×4) | 16 |

```bash
# in saphana-write/
task bench:load COUNT=1000000    # fill Kafka topic once; reused by both flavors
task bench:matrix OUT=rpcn_write.txt
task bench:kc:matrix OUT=kc_write.txt
```

> KC write note: messages are plain JSON (no Schema Registry). KC auto-creates `BENCH_WRITES_KC` with inferred types (`ts` stored as `NVARCHAR(30)` rather than `TIMESTAMP`). Throughput comparison is still valid.

---

## Tests

### `bench:test:skip-existing` (incrementing mode)

Verifies that the connector does not re-read rows that existed before it started.

**Steps:**
1. Creates a fresh `BENCH_ORDERS_INC` table and inserts 1,000 rows.
2. Queries `MAX(ID)` and starts the connector with `incrementing_initial_value` set to that value — the connector will only emit rows with `ID > MAX`.
3. Waits 5 seconds and asserts Kafka has **0 messages** (pre-existing rows skipped).
4. Inserts 100 new rows while the connector is running.
5. Waits up to 15 seconds and asserts Kafka has **100 messages** (net-new rows picked up).

```bash
cd saphana-read/incrementing
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="MY_SCHEMA"
task bench:build
task bench:test:skip-existing
```

Expected output:
```
PASS: 0 messages produced for 1000 pre-existing rows (HWM correctly applied)
PASS: 100 / 100 new rows picked up by connector
ALL TESTS PASSED
```
