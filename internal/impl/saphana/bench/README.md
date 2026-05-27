# SAP HANA Benchmark Suite

Benchmarks SAP HANA `sap_hana` input throughput across different modes.

## What each folder measures

| Folder | Mode | Description |
|---|---|---|
| `saphana-read/bulk/` | `bulk` | Read all rows once, one message per row → Kafka |

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
