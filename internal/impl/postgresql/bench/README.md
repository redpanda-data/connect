# PostgreSQL Benchmark

Two benchmark scenarios:

1. **CDC / Snapshot** — `postgres_cdc` input reading directly from PostgreSQL via logical replication
2. **Kafka → PostgreSQL** — Redpanda Connect vs Kafka Connect (JDBC Sink) writing from Kafka into PostgreSQL

See [`docs/benchmark-results/postgres.md`](../../../../docs/benchmark-results/postgres.md) for full results.

---

## CDC / Snapshot Benchmarks

### Prerequisites

```bash
# macOS
brew install postgresql

# Ubuntu/Debian
sudo apt install postgresql-client
```

### Setup

```bash
task postgres:up      # start PostgreSQL 16 with wal_level=logical
task psql:create      # create tables
```

### Running

```bash
# Load a dataset and benchmark in one command
task bench:cart  CORES=4 BATCH=5000   # 10M small rows (~600 B) — CPU bound
task bench:users CORES=2 BATCH=1000   # 150K large rows (~625 KB) — I/O bound
```

- `CORES` — sets `GOMAXPROCS` (omit for unbounded)
- `BATCH` — sets `batching.count` (default 1000)

Both tasks truncate, load the dataset, drop the replication slot, and run the benchmark.

### CDC mode

```bash
# terminal 1 — start connector (creates replication slot)
task bench:run:cdc

# terminal 2 — insert data while connector is running
task psql:data:users
```

### Teardown

```bash
task postgres:down    # stops and removes the container — no leftover files
```

---

## Kafka → PostgreSQL Benchmarks

Infrastructure is provided by the [`kafka-connector/`](./kafka-connector/) subfolder (Kafka + PostgreSQL + Kafka Connect in Docker).

### Setup

```bash
cd kafka-connector
task up
```

### Load data once, run multiple times

```bash
# From bench/ (this folder):

# Step 1 — produce 10M messages to Kafka (run once per comparison set)
task bench:kafka:load COUNT=10000000

# Step 2 — run Redpanda Connect benchmark (repeat with different params)
task bench:kafka MAX_IN_FLIGHT=64
task bench:kafka MAX_IN_FLIGHT=128 CORES=4
```

### Run Kafka Connect benchmark

```bash
cd kafka-connector
task bench:run COUNT=10000000
```

### Teardown

```bash
cd kafka-connector
task down
```
