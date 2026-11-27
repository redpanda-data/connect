# Aerospike output: local write benchmark

Measures `output.aerospike` writing 512-byte records to one Enterprise node in Docker. There is no broker in this pipeline. `generate` produces the records, the `benchmark` processor logs the rate, and `output.aerospike` writes them, so the logged rate is the rate the output accepts.

Results are in [`docs/benchmark-results/aerospike.md`](../../../../docs/benchmark-results/aerospike.md).

## Record

| Field | Value |
|---|---|
| Namespace / set | `test` / `players` |
| Key | unique integer `key` |
| Bin | `name`, 512 bytes |
| Operation | `replace` |
| TTL | 15,000 seconds |
| Not stored | `key`, `namespace`, `ttl` |

The namespace is 4 GiB of memory, which holds a 2,000,000-record run of this record. Docker Desktop must allow the container at least 4 GiB plus Aerospike's own overhead. The compose file caps it at 6 GiB.

## Prerequisites

- Docker, with a pull of `aerospike/aerospike-server-enterprise:8.1` allowed.
- Go, from a checkout where `task deps` has already run.
- `task` (go-task).

The server image ships its feature key at `/etc/aerospike/features.conf`. No host key is needed.

`task run` builds `main.go` in this directory through the community `service.RunCLI` entry point. That binary registers `generate`, the `benchmark` processor, Prometheus, and `output.aerospike`. It does not compile `cmd/redpanda-connect`, which pulls in every community connector. The first build takes a few minutes.

## How to run

```bash
cd internal/impl/aerospike/bench
task bench COUNT=2000000 BATCH=10 CORES=1
```

`task bench` starts the node, waits until the cluster reports stable, writes `COUNT` records, and removes the node even when the write fails. The wait gives up after 60 tries, about two minutes, and the task fails instead of hanging.

| Variable | Default | What it sets |
|---|---|---|
| `COUNT` | `100000` | Records the `generate` input produces |
| `BATCH` | `10` | `output.aerospike` `batching.count` |
| `PERIOD` | `200ms` | `output.aerospike` `batching.period` |
| `CORES` | `1` | `GOMAXPROCS` for the Connect process |

To keep the node up between runs:

```bash
task up
task run COUNT=2000000 BATCH=10 CORES=1
task down
```

`counter()` starts at 1 on every `task run`, so a second run overwrites the same keys. Run `task down` and then `task up` to start from an empty namespace.

## Reproducibility

The node is pinned in `docker-compose.yml`: `cpuset: "0-3"` (four cores) and `mem_limit: 6g`. Connect runs on the host, so its CPU count is set with `CORES` (`GOMAXPROCS`) and its memory with `GOMEMLIMIT=1GiB`, which the Taskfile sets. Connect is not pinned to cores on macOS. Run each configuration more than once and report the spread, not one run.

## Expected output

```text
INFO rolling stats: <msg/sec>, <MB/sec>
INFO total stats: <msg/sec>, <MB/sec>
```

A run shorter than the 1s interval prints only `total stats`. On a longer run, read the steady `rolling stats` lines and skip the first, which includes client warmup. Unique keys avoid hot-key `KEY_BUSY`. `COUNT=100000` shows that the pipeline runs. It is not a rate to quote.

Prometheus and pprof listen on `127.0.0.1:4195` while `task run` is in progress, since `http.debug_endpoints` is on. With the profiling stack in `resources/docker/profiling`:

```bash
cd resources/docker/profiling
task profile:cpu
task profile:mem
```
