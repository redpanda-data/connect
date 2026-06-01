# RDS quirks for CDC benches

What RDS does differently from upstream Postgres/MySQL, where it bites, how to handle it. Only needed when adding a CDC source bench.

## Postgres: parameter group settings

RDS exposes a different set of GUCs than self-hosted Postgres. Settings that look familiar in `postgresql.conf` may not be settable.

**Settable for logical replication:**

| Postgres GUC | RDS parameter | Required value |
|--------------|---------------|----------------|
| `wal_level` | `rds.logical_replication` | `"1"` (boolean as string) |
| `max_wal_senders` | `max_wal_senders` | ≥ 10 (10 is fine) |
| `max_replication_slots` | `max_replication_slots` | ≥ 10 |

**Not settable on RDS:** `wal_level` directly, `wal_keep_size` (use `rds.logical_replication`).

Reference: `terraform/modules/rds-postgres/main.tf` parameter group. Trap: [traps.md#rds-logical-replication](traps.md#rds-logical-replication).

## Postgres: postgres_cdc TLS field

The Redpanda Connect `postgres_cdc` input uses `service.NewTLSField`, which has no `enabled:` toggle (unlike `NewTLSToggledField`). RDS rejects unencrypted replication connections, so TLS must be enabled — by setting the inner fields directly.

**In scenario YAML:**

```yaml
input:
  postgres_cdc:
    dsn: ${POSTGRES_DSN}
    tls:
      skip_cert_verify: true   # RDS-internal CA isn't in the runner image
    # NO `enabled: true` — field doesn't exist
```

Trap: [traps.md#postgres-cdc-tls](traps.md#postgres-cdc-tls).

## RDS instance class minimums

CDC benches under sustained 50K+ msg/sec require:

| Engine | Minimum instance class | Storage | IOPS |
|--------|------------------------|---------|------|
| Postgres | `db.r6g.2xlarge` | 400 GB gp3 | 12000 |
| MySQL | `db.r6g.2xlarge` | 400 GB gp3 | 12000 |

Smaller instances will appear to work at low CPU points (vCPU=1) and degrade silently as the sweep ramps. The `iops` parameter is **required if `storage_gb >= 400`** and **forbidden if `storage_gb < 400`** for the Postgres engine — see `bench-debugging-history` #28.

## Backup window collisions (false alarm)

An earlier hypothesis blamed RDS auto-backup window collisions for the vCPU-8 mysql degradation pattern. **This was wrong** — pinning `backup_window` to off-hours did not change the pattern. See `bench-debugging-history` #25. Real cause is RDS-internal at sustained ~5min of 150K writes/sec into a single table (gp3 throttling, binlog flush back-pressure, or per-instance IOPS soft ceiling).

When sizing a new CDC bench, do not assume the backup window is the bottleneck. Profile producer-side throughput first.
