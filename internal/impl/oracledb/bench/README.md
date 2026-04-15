# Benchmarking Oracle CDC Component

Benchmark demonstrating throughput of Redpanda's Oracle CDC Connector, with an optional Debezium comparison.

## Prerequisites

- Docker
- An Oracle container registry account — accept the terms at https://container-registry.oracle.com before pulling

## Redpanda Connect Benchmark

Two modes are supported. **CDB mode** (preferred) connects via the CDB root and supports multi-tenant databases. **PDB mode** connects directly via the PDB service.

### CDB mode (preferred)

#### 1. Start Oracle

```bash
task oracledb:up
```

Wait for the database to be ready (check with `task oracledb:logs` — look for `DATABASE IS READY TO USE!`).

#### 2. Enable ARCHIVELOG mode (required for LogMiner)

```bash
task oracledb:archivelog
```

#### 3. Create and configure the pluggable database

```bash
task pdb:create
task pdb:setup
task cdb:setup
```

#### 4. Start Redpanda Connect

```bash
go run ../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

#### 5. Generate test data

In a separate terminal, run one or more of the following:

```bash
task sqlcl:pdb:data:users      # inserts rows into TESTPDB
task sqlcl:pdb:data:products   # inserts rows into TESTPDB
```

Redpanda Connect will stream the CDC events via LogMiner as data is inserted.

#### 6. Clear checkpoint cache between runs

```bash
task sqlcl:cdb:drop-cache
```

---

### PDB mode

#### 1. Start Oracle

```bash
task oracledb:up
```

Wait for the database to be ready (check with `task oracledb:logs` — look for `DATABASE IS READY TO USE!`).

#### 2. Enable ARCHIVELOG mode (required for LogMiner)

```bash
task oracledb:archivelog
```

#### 3. Create and configure the pluggable database

```bash
task pdb:create
task pdb:setup
```

#### 4. Start Redpanda Connect

```bash
go run ../../../../cmd/redpanda-connect/main.go run ./benchmark_config.yaml
```

#### 5. Generate test data

In a separate terminal, run one or more of the following:

```bash
task sqlcl:pdb:data:users      # inserts rows into TESTPDB
task sqlcl:pdb:data:products   # inserts rows into TESTPDB
```

Redpanda Connect will stream the CDC events via LogMiner as data is inserted.

#### 6. Clear checkpoint cache between runs

```bash
task sqlcl:pdb:drop-cache
```

## Recording Results

After running the benchmark, record your results in [`docs/benchmark-results/oracledb-cdc.md`](../../../../docs/benchmark-results/oracledb-cdc.md). Append a new dated section with environment details, dataset, throughput numbers, and observations. See [`docs/benchmarking.md`](../../../../docs/benchmarking.md) for the full guide.
