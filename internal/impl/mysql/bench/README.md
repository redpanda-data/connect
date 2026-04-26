# MySQL Benchmark Suite

This folder benchmarks MySQL throughput across two directions and multiple tools.

## What each folder measures

| Folder | Direction | Tool | Redpanda Connect equivalent |
|---|---|---|---|
| `mysql-read/debezium/` | MySQL → Kafka | Debezium source connector | `mysql_cdc` input |
| `mysql-read/rpcn/` | MySQL → Kafka | Redpanda Connect | `mysql_cdc` input |
| `mysql-write/jdbc-sink/` | Kafka → MySQL | Confluent JDBC Sink connector | `sql_insert` output |
| `mysql-write/rpcn/` | Kafka → MySQL | Redpanda Connect | `sql_insert` output |

## What this folder measures

This root benchmark measures **pure `mysql_cdc` read throughput** — no Kafka involved.
The output is dropped immediately, so the number reflects the MySQL read ceiling with no downstream bottleneck.
This is what produced the **191K msg/sec peak** in the snapshot results.

Key differences from the subfolders:
- **No Kafka** — output is `drop`, not a real destination
- **Three tables** (`users`, `products`, `cart`) — subfolders use only `cart`
- **Throughput measured via Prometheus metrics** on the debug endpoint — subfolders measure Kafka offsets externally

See [`docs/benchmark-results/mysql-cdc.md`](../../../../docs/benchmark-results/mysql-cdc.md) for full results.

---

## Prerequisites

Docker must be running. No MySQL client needed — all commands run inside the container.

## Start

```bash
task up            # start MySQL 8.0 with binlog enabled (waits until ready)
task mysql:create  # create tables (users, products, cart)
```

## Load data

```bash
task bench:load:cart COUNT=10000000   # 10M small rows (~600 B each)
```

`COUNT` is optional — defaults to 10M. Load only once per comparison set; `bench:run` resets the checkpoint without touching the data.

## Run benchmark

```bash
task bench:run                          # default: unbounded cores, batch=1000
task bench:run CORES=4 BATCH=5000       # set GOMAXPROCS and batching.count
```

The benchmark auto-exits after the snapshot completes and prints a `total stats` summary line with the average throughput.

## Run all combinations

```bash
task bench:load:cart COUNT=10000000

for cores in 1 2 4 8; do
  for batch in 1000 5000 10000; do
    task bench:run CORES=$cores BATCH=$batch
  done
done
```

## CDC mode (live streaming)

```bash
# terminal 1 — start connector, streams from current binlog position
task bench:run:cdc

# terminal 2 — insert rows while connector is running
task bench:load:cart COUNT=100000
```

## Stop

```bash
task down          # stops and removes the container — no leftover data
```
