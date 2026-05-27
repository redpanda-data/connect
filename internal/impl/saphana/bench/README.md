# SAP HANA Benchmark Suite

Benchmarks SAP HANA `sap_hana` input throughput across different modes.

## What each folder measures

| Folder | Mode | Description |
|---|---|---|
| `saphana-read/bulk/` | `bulk` | Read all rows once, one message per row → Kafka |
| `saphana-read/incrementing/` | `incrementing` | Poll for net-new rows using HWM column, connector stays running → Kafka |

## Prerequisites

- Docker running (Kafka only — SAP HANA is external)
- `HANA_DSN` env var set, e.g. `export HANA_DSN="hdb://USER:PASS@host:30015"`

## Quick start

```bash
cd saphana-read/bulk

export HANA_DSN="hdb://USER:PASS@host:30015"

task up                          # start Kafka
task bench:build                 # build binaries
task bench:load COUNT=1000000    # create table + load 1M rows
task bench:run                   # run benchmark, print throughput
task down                        # stop Kafka
```

## Run all core combinations

```bash
task bench:all OUT=results.txt
```

## Tests

### `bench:test:skip-existing` (incrementing mode)

Verifies that the connector does not re-read rows that existed before it started.

**Steps:**
1. Creates a fresh `BENCH_ORDERS_INC` table and inserts 1 000 rows.
2. Queries `MAX(ID)` and starts the connector with `incrementing_initial_value` set to that value — the connector will only emit rows with `ID > MAX`.
3. Waits 5 seconds and asserts Kafka has **0 messages** (pre-existing rows skipped).
4. Inserts 100 new rows while the connector is running.
5. Waits up to 15 seconds and asserts Kafka has **100 messages** (net-new rows picked up).

Located at `internal/impl/saphana/bench/saphana-read/incrementing/Taskfile.yaml`, task `bench:test:skip-existing`.

```bash
cd internal/impl/saphana/bench/saphana-read/incrementing
export HANA_DSN="hdb://USER:PASS@host:30015"
export HANA_SCHEMA="YOUR_SCHEMA"
task bench:build
task bench:test:skip-existing
```

Expected output:
```
PASS: 0 messages produced for 1000 pre-existing rows (HWM correctly applied)
PASS: 100 / 100 new rows picked up by connector
ALL TESTS PASSED
```
