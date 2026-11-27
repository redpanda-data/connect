# Aerospike output benchmark results

Local write benchmark for `output.aerospike`: [`internal/impl/aerospike/bench/`](../../internal/impl/aerospike/bench/). The pipeline has no broker. `generate` writes records straight to the output, and the logged rate is the rate the output accepts.

## 2026-10-22 — local Docker, 2,000,000 records, unpinned

- **Command:** `task run COUNT=2000000 BATCH=10 CORES=1` after `task up`

### Environment

- **Host:** MacBook Pro, Apple M1 Pro, 10 cores, 32 GB RAM, macOS.
- **Aerospike:** `aerospike/aerospike-server-enterprise:8.1`, one node, memory engine, replication factor 1, namespace `test` with `data-size 4G`. No CPU or memory limit.
- **Connect:** `GOMAXPROCS=1`, no broker.

### Dataset

2,000,000 records, unique integer keys, 512-byte `name` bin, `replace`, TTL 15,000 seconds. About 1.0 GiB stored. The processor counts about 566 bytes per message, including the fields that are not stored.

### Configuration

Output batching 10 records or 200ms.

### Results

| | msg/sec | MB/sec |
|---|---|---|
| Steady rolling samples | 22,500–32,200, mostly 27,000–29,000 | 13–18 |
| Total | 28,210 | 16 |

Runtime 71 seconds.

### Observations

No write errors. The input finished, so the total covers the full 2,000,000 records.

## 2026-11-02 — local Docker, 2,000,000 records, unpinned

- **Command:** `task bench COUNT=2000000 BATCH=10 CORES=1`

### Environment

Same as 2026-10-22.

### Dataset

Same as 2026-10-22.

### Configuration

Same as 2026-10-22.

### Results

| | msg/sec | MB/sec |
|---|---|---|
| Total | 25,225 | 14 |

### Observations

Within the spread of the 2026-10-22 run.

## 2026-11-11 — local Docker, 2,000,000 records, pinned

- **Command:** `task bench COUNT=2000000 BATCH=10 CORES=1`, twice.

### Environment

- **Host:** same as 2026-10-22. Docker Desktop VM with 10 CPUs and 16 GB.
- **Aerospike:** as before, with the container pinned to cores `0-3` (`cpuset`) and `mem_limit: 6g`.
- **Connect:** built with Go 1.27.3 for arm64, `GOMAXPROCS=1`, `GOMEMLIMIT=1GiB`. Connect runs on the host and is not pinned to cores.

### Dataset

Same as 2026-10-22.

### Configuration

Same as 2026-10-22.

### Results

| Run | msg/sec | MB/sec |
|---|---|---|
| 1 | 25,784 | 15 |
| 2 | 32,428 | 18 |

Steady rolling samples ranged from about 13,000 to 35,000 msg/sec in run 1 and from about 24,000 to 39,000 in run 2.

### Observations

No write errors in either run, and no `INVALID_NODE_ERROR` at startup. The totals differ by about 25% on the same configuration, so treat one run as a sample. On this laptop the output sustains roughly 26,000 to 32,000 msg/sec at one core and batch 10.
