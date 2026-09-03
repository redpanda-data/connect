# MongoDB CDC — Redpanda Connect vs Kafka Connect (Debezium)

Head-to-head of the Connect `mongodb_cdc` input against Debezium's MongoDB
connector, both consuming MongoDB **change streams** from the same self-hosted
single-node replica set (`rs0`) on EC2. Sweep over 1/2/4/8 vCPU on identical
hardware; broker-side throughput is the canonical, apples-to-apples metric.

_Result recorded 2026-07-17 (commits `156a11081`, `f1ea720cd`). Raw per-run JSON
lives under `benchmarking/aws/results/mongodb/` (gitignored run artifacts)._

## TL;DR

- **Connect wins on both speed and stability.** It rises to ~27 MB/s (broker)
  and holds steady; Debezium is slower and erratic.
- **MongoDB CDC does not scale with connector vCPU** — both engines flatline
  from 2 vCPU. The bottleneck is MongoDB's **single change-stream cursor**, not
  the connector. No connector config raises it; only a sharded cluster would.

## Head-to-head (1 vCPU smoke + 8/8 sweep)

Broker-derived median MB/s is the fair cross-engine number (both measured
identically at the broker). Connect's self-reported rate is shown for context.

| vCPU | Connect (broker) | Debezium/KC (broker) | Connect (self-report) |
|------|------------------|----------------------|-----------------------|
| 1    | 23.2             | 19.4                 | 26                    |
| 2    | 24.4             | 16.6                 | 33                    |
| 4    | 26.5             | 26.5                 | 33                    |
| 8    | 26.7             | 17.2                 | 33                    |

**Reading it:** Connect climbs to ~27 MB/s and stays flat and steady (its
self-report holds a clean 33 from 2 vCPU up). Debezium bounces between ~17 and
~26 MB/s with no upward trend — its `Unable to acquire buffer lock, buffer queue
is likely full` warnings surface as throughput jitter. The two nearly tie at
4 vCPU, but Connect leads at every other point and is far more stable.

## Key finding: single-cursor ceiling

A MongoDB change stream is **one ordered cursor** served largely single-threaded
per stream on the server side (tailing the oplog and materializing change
events). Consequences:

- **Flat past 2 vCPU.** At 1 vCPU the *connector* is the bottleneck (it can't
  fully drain the cursor); by 2 vCPU it outpaces the cursor, so the **cursor**
  becomes the bottleneck and extra connector cores do nothing.
- **Consumer-bound, not source-fed-bound.** mongod ingested ~230 MB/s of writes
  (that rate wraps a small oplog); the stream only *delivers* ~27, so the read
  path — not the write feed — is the limit.
- **No connector config raises the ceiling.** `mongodb_cdc` streams via a single
  database-level `db.Watch` (`internal/impl/mongodb/cdc/input.go`); multiple
  `collections` just add a `$match` filter to that one cursor — they do **not**
  fan out to parallel cursors. Batch-size / checkpoint tuning only helps *reach*
  the ceiling (mainly the 1-vCPU point).
- **The only lever is a sharded cluster.** Against a sharded MongoDB, `mongos`
  merges **per-shard cursors that tail in parallel**, so aggregate throughput
  scales with shard count (Debezium can further use per-shard `tasks.max > 1`).
  This is infrastructure, not a config flag.

This is fundamentally different from the Postgres/MySQL sweeps (logical
replication / binlog), which are consumer-CPU-bound and therefore *do* scale
with vCPU.

## Methodology & caveats

- **Source:** self-hosted `mongod` single-node replica set on `im4gn.4xlarge`
  (dbPath on local NVMe), 500 GB oplog. A single-node RS is a fair proxy for CDC
  throughput on a production 3-node RS — a change stream reads the one primary's
  oplog regardless of node count.
- **Representativeness:** most production MongoDB runs an unsharded replica set,
  so this single-cursor ceiling is what typical deployments would actually hit.
- **Workload:** the seeder cycles a 4096-entry pool of distinct random payloads
  so change events are incompressible for both engines (a single reused payload
  compresses on Connect's wire but not Debezium's, which skewed the broker-byte
  metric ~13x before the fix).
- **Rate:** saturating (200K/sec) with a large oplog to hold the consumer's
  backlog for the whole window without wrapping (an oplog wrap invalidates the
  change stream and collapses throughput to zero). The `[1]`-only smoke uses a
  lower sustainable rate for the same reason.
- **Connect self-report (33) vs broker (~27):** a ~20% metric-definition offset
  (the benchmark processor counts payload bytes; the broker counts on-wire
  record bytes). Not compression (batches are incompressible) and not a
  cross-engine fairness issue — the head-to-head uses broker-for-both.
- **KC noise:** Debezium's per-point numbers are volatile; treat the KC column
  as "roughly flat ~17–26 with jitter," not a clean curve.

## Reproduce

```bash
aws-vault exec <bench-profile> -- \
  env REDPANDA_LICENSE_FILEPATH=$PWD/rpcn.license \
  task aws:bench scenario=mongodb/orders-cdc
```

Long (~2.3h) unattended sweeps need a fresh `aws sso login` immediately before
launch (SSO-token expiry mid-run causes a hang) and, when driven via automation,
a detached (`nohup`) launch so the job isn't killed. See `bench-mongodb-prep`
notes for the operational gotchas.
