# Benchmarking Oracle CDC Component

Benchmark demonstrating throughput of Redpanda's Oracle CDC Connector, with an optional Debezium comparison.

## Prerequisites

- Docker
- [Task](https://taskfile.dev)
- Go, to build and run Redpanda Connect from source
- An Oracle container registry account — accept the terms at https://container-registry.oracle.com before pulling

## Redpanda Connect Benchmark

Two modes are supported. **CDB mode** (preferred) connects via the CDB root and supports multi-tenant databases. **PDB mode** connects directly via the PDB service.

`benchmark_config.yaml` runs in `snapshot_only` mode: Redpanda Connect snapshots the tables, writes every row to `benchmark_results.json`, then stops. Load the test data **before** starting it.

### CDB mode (preferred)

#### 1. Set up Oracle

```bash
task bench:up
```

This starts the Oracle container, enables ARCHIVELOG mode (required for LogMiner), creates the `TESTPDB` pluggable database and its tables, and creates the CDB common users. Follow progress with `task oracledb:logs`. If a step fails because the database wasn't ready yet, wait for `DATABASE IS READY TO USE!` and rerun the remaining steps individually (`task oracledb:archivelog`, `task pdb:create`, `task pdb:setup`, `task cdb:setup`).

#### 2. Load test data

Run one or more of the following. `TOTAL` sets the number of rows to insert; it's optional and defaults to the value shown:

```bash
task sqlcl:pdb:data:users TOTAL=1000000
task sqlcl:pdb:data:products TOTAL=50000
task sqlcl:pdb:data:cart TOTAL=10000000
```

Each script gathers optimizer statistics on its table once the data is loaded.

#### 3. Run the benchmark

```bash
task bench:run
```

Throughput is logged every second as `rolling stats`, followed by a `total stats` line when the snapshot completes. The output is also written to `benchmark_logs.log`.

#### 4. Reset between runs

```bash
task bench:reset
```

This drops the CDC checkpoint cache, so the next run snapshots again, and deletes `benchmark_results.json` and `benchmark_logs.log`. It's safe to run when they don't exist.

#### 5. Tear down

```bash
task bench:down
```

---

### PDB mode

Follow the CDB mode steps with these changes:

1. In `benchmark_config.yaml`, use the commented-out `TESTPDB` `connection_string` and remove `pdb_name`. The CDB common users that `bench:up` creates are simply unused.
2. Between runs, also drop the PDB checkpoint cache, since `bench:reset` only drops the CDB one:

   ```bash
   task sqlcl:pdb:drop-cache
   ```

---

### Benchmarking streaming

To benchmark LogMiner streaming instead of the snapshot, set `snapshot_mode` in `benchmark_config.yaml` to `none` (stream only) or `snapshot_and_stream`. Start Redpanda Connect with `task bench:run` first, then load the test data in a separate terminal, so the inserts are streamed as they're committed.

## Expected Output

While the snapshot runs, the `benchmark` processor logs throughput every second, and a summary when it completes:

```
level=info msg="Starting snapshot of 3 table(s) using 50 configured readers"
level=info msg="rolling stats: 573852 msg/sec, 152 MB/sec"
level=info msg="rolling stats: 0 msg/sec, 0 B/sec"
level=info msg="rolling stats: 624577 msg/sec, 166 MB/sec"
level=info msg="Table snapshot completed, 5000000 rows processed" src_table=TESTDB.USERS
level=info msg="Completed running snapshot process"
level=info msg="total stats: 151325.249406 msg/sec, 40 MB/sec"
```

These lines are from a 5,000,000-row `USERS` table on a laptop, with `max_parallel_snapshot_tables: 50`. Seconds of `0 msg/sec` between bursts are normal with the `file` output: with `batching.period: 1s`, each batch holds about a second of rows, and the next batch isn't counted until the file output has written the previous one. Swap the output for `drop: {}` to measure the input on its own.

## Recording Results

After running the benchmark, record your results in [`docs/benchmark-results/oracledb-cdc.md`](../../../../docs/benchmark-results/oracledb-cdc.md). Append a new dated section with environment details, dataset, throughput numbers, and observations. See [`docs/benchmarking.md`](../../../../docs/benchmarking.md) for the full guide.
