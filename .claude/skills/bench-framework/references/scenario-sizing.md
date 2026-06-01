# Scenario sizing

Three traps that bit during postgres + mysql sweeps. Apply when writing a new scenario YAML.

<a name="workload-rate-shape"></a>
## Workload `--rate` is per-table-per-second, not total

`scenarios/<x>/<scenario>.yaml::workload.write_rate_per_sec` is interpreted by the seeder's `workload` subcommand as **per-table-per-second**. With N tables in `dataset.tables`, total throughput = N × `write_rate_per_sec`.

When sizing a new scenario:

- Single table: `write_rate_per_sec` = total target rate.
- Multi-table (e.g. `tables: [orders_a, orders_b, orders_c, orders_d]`): divide your target by 4.

Trap: [traps.md#workload-rate-per-table](traps.md#workload-rate-per-table).

<a name="truncate-between-points"></a>
## TRUNCATE between sweep points required

Without TRUNCATE in `reset:`, the source table grows across sweep points. By vCPU=8 in a 4-point sweep, the orders table can hit ~300M rows. Per-insert latency on the b-tree stretches; producer stalls; throughput collapses.

**Recipe (postgres-shaped):**

```yaml
dataset:
  initial_rows: 0          # TRUNCATE-between-points makes seed pointless
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows
reset:
  - sql: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"
  - sql: "SELECT pg_drop_replication_slot('kc_bench_slot') FROM pg_replication_slots WHERE slot_name='kc_bench_slot'"
  - sql: "TRUNCATE TABLE orders"
```

**Recipe (mysql-shaped):**

```yaml
dataset:
  initial_rows: 0
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows
reset:
  - sql: "TRUNCATE TABLE orders"
```

MySQL doesn't have slots to drop; the per-vCPU connector name in `kcConnectorSpecs` handles offset isolation instead.

Trap: [traps.md#truncate-between-points](traps.md#truncate-between-points).

## Observed producer-side ceilings (`db.r6g.2xlarge`)

These are real ceilings discovered while pushing for headroom. Use as upper bounds when sizing new CDC scenarios:

| Engine | Sustained ceiling | Peak | Notes |
|--------|-------------------|------|-------|
| Postgres single-table | ~50K msg/sec | ~108 MB/s instantaneous | b-tree contention, WAL flush serialisation |
| MySQL single-table | ~75K msg/sec | ~114 MB/s instantaneous | binlog flush is more efficient than WAL |
| `write_rate_per_sec: 150000` on either | ~50-75K achieved | Same peaks | Producer-bound, not Connect-bound |

If you need to push past these for a fair "find the real ceiling" sweep, the only fixes are:
- Multi-table workload (e.g. 4 × orders_a..orders_d)
- Bigger source instance (`db.r6g.4xlarge`+)
- Different write-amplification model (UPDATEs instead of INSERTs)

## Sizing checklist for a new scenario

- [ ] `write_rate_per_sec` accounts for table count (per-table-per-sec)
- [ ] `dataset.initial_rows: 0` + TRUNCATE in `reset:` between points
- [ ] Reset drops Connect's slot AND KC's slot if both engines being swept
- [ ] `cpu_points: [1, 2, 4, 8]` unless you have reason to skip 8 (long wall-clock vs. 4h orphan TTL)
- [ ] `infra.source.instance_class` ≥ `db.r6g.2xlarge` for sustained workloads
- [ ] Runner = `c8g.4xlarge` (current best price/perf for the bench framework)
