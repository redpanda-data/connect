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

**Symptoms you're producer-bound, not Connect-bound:**
- Wide spread between p5 and peak in the same sweep point (e.g. p5=44, peak=98 — producer bursting then back-pressuring).
- Connect's CPU is well below `vCPU × 100%` (e.g. 21% utilization at 8 vCPU). Confirmed via Prometheus snapshots.
- Sustained throughput dips mid-window (~3-5 min in) — gp3 burst credit exhaustion.
- Cross-engine numbers move in lockstep (both Connect AND KC dip at the same vCPU) — the bottleneck is upstream of both.

### How to push past the ceiling

Ranked by effort/impact when investigating Connect's actual ceiling:

#### 1. Bigger source instance (easiest)

Single-line scenario edit:

```yaml
infra:
  source:
    instance_class: db.r6g.4xlarge   # was 2xlarge
    storage_gb: 800                   # 4xlarge needs more headroom
    iops: 24000                       # 2× IOPS budget
```

**Why it works:** gp3 storage has per-instance-class throughput ceilings tied to baseline burst credits. `db.r6g.4xlarge` has ~2× the IOPS baseline and 2× the network throughput. Most sustained-degradation patterns are gp3 credit exhaustion.

**Cost:** ~2× RDS hourly. Adds ~$2-4 to a full sweep.

**Verdict:** Try first. No code changes, no scenario restructure, diagnostic in one bench.

#### 2. Multi-table workload (proper fix)

Spread writes across 4-8 tables to break single-table contention:

```yaml
dataset:
  tables: [orders_a, orders_b, orders_c, orders_d]

workload:
  write_rate_per_sec: 37500   # 150K total / 4 tables (PER-TABLE-PER-SEC — see Trap 7)

pipeline:
  input:
    mysql_cdc:
      tables: [orders_a, orders_b, orders_c, orders_d]

reset:
  - sql: "TRUNCATE TABLE orders_a"
  - sql: "TRUNCATE TABLE orders_b"
  - sql: "TRUNCATE TABLE orders_c"
  - sql: "TRUNCATE TABLE orders_d"
```

**Why it works:** B-tree contention and WAL/binlog flush serialisation are **per-table**. With 4 tables, the 16-worker seeder rounds-robin so each table sees ~4 workers, each table's b-tree updates serialise independently.

**Caveat:** Audit the seeder first. `seeders/cdc-rows/sql.go::workload()` accepts a tables list — verify workers actually round-robin across tables instead of all hammering tables[0]. If they don't, fix that before relying on multi-table results.

**Cost:** Same instance, just better utilization.

**Verdict:** Best fix when you actually want to find Connect's per-vCPU ceiling.

#### 3. io2 storage instead of gp3

Provisioned IOPS — no burst credits, no soft throttling.

```yaml
infra:
  source:
    instance_class: db.r6g.2xlarge
    storage_type: io2          # NOT a scenario field yet — needs rds-mysql / rds-postgres module update
    storage_gb: 400
    iops: 20000                # io2 supports up to 256K IOPS
```

**Why it works:** gp3 has burst-based throttling — credits regenerate, but heavy sustained workloads exhaust them. io2 is **provisioned**: you pay for IOPS upfront, get exactly that rate, sustained.

**Effort:** Currently `terraform/modules/rds-{postgres,mysql}/main.tf` hardcode `storage_type = "gp3"`. Add a variable.

**Cost:** io2 is ~3-5× per-GB + per-IOPS charge. For a 3-hour bench, <$1 extra. Long-running infra (>24h) gets expensive.

**Verdict:** Good middle ground between "just bigger instance" and "rearchitect the workload."

#### 4. Aurora (different storage architecture)

Aurora Postgres/MySQL have log-structured storage that handles sustained writes very differently — no WAL/binlog flush back-pressure in the same way.

**Effort:** New module entirely (`terraform/modules/aurora-mysql/` or aurora-postgres/). Aurora's API and behavior differ enough that this isn't a drop-in.

**Verdict:** Only worth it if you want RDS-vs-Aurora as a finding. Significant added complexity.

#### 5. Different write-amplification model (last resort)

UPDATEs instead of INSERTs — no b-tree growth, just in-place row updates.

**Caveat:** Less realistic for most CDC use cases. INSERT-heavy workloads are the common shape. Only use this if you're specifically benching update-heavy scenarios.

### Recommended sequence when investigating a ceiling

1. Run the baseline at `db.r6g.2xlarge` / single-table.
2. Check Prometheus: is Connect's CPU under-utilized? If yes, you're producer-bound.
3. Try `db.r6g.4xlarge` (option 1). If the ceiling moves up significantly (e.g. 95 → 150 MB/s), it was gp3-bound.
4. If 4xlarge still plateaus, switch to multi-table (option 2). Connect's per-vCPU ceiling will surface.
5. Only consider io2 or Aurora if 1+2 still aren't enough.

## Sizing checklist for a new scenario

- [ ] `write_rate_per_sec` accounts for table count (per-table-per-sec)
- [ ] `dataset.initial_rows: 0` + TRUNCATE in `reset:` between points
- [ ] Reset drops Connect's slot AND KC's slot if both engines being swept
- [ ] `cpu_points: [1, 2, 4, 8]` unless you have reason to skip 8 (long wall-clock vs. 4h orphan TTL)
- [ ] `infra.source.instance_class` ≥ `db.r6g.2xlarge` for sustained workloads
- [ ] Runner = `c8g.4xlarge` (current best price/perf for the bench framework)
