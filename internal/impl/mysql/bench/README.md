# MySQL CDC Benchmark

Benchmark for `mysql_cdc` input — measures snapshot throughput reading from MySQL via binlog streaming.

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
