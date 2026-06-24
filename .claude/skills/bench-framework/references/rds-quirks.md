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

## SQL Server: enabling CDC on RDS

RDS SQL Server exposes `MSSQLSERVER` as the engine name. The Terraform module uses `engine = "sqlserver-se"` (Standard Edition) or `"sqlserver-ee"` (Enterprise). CDC is NOT enabled at RDS parameter level — instead it is enabled via T-SQL commands.

**Required T-SQL to enable CDC on the database and table (run once, not in reset):**

```sql
-- 1. Enable CDC on the database
EXEC sys.sp_cdc_enable_db;

-- 2. Enable CDC on the target table
EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name   = 'orders',
  @role_name     = null;
```

These commands are idempotent if run again but should go in the **first-run seed script**, not the reset block. The reset block only needs disable+truncate+re-enable (see `reset.sql.tmpl`).

**RDS SQL Server parameters** (set in the module's parameter group):

| Setting | Value | Why |
|---------|-------|-----|
| `rds.sqlserver_audit` | N/A | Not needed for CDC |

No parameter group settings are required for CDC on RDS SQL Server — it is enabled at the DB/table level via T-SQL above.

**RDS engine string and family for Terraform:**

```hcl
engine         = "sqlserver-se"          # Standard Edition
engine_version = "15.00.4415.2.v1"      # SQL Server 2019 — check latest via aws rds describe-db-engine-versions
family         = "sqlserver-se-15.0"     # parameter group family
```

Unlike Postgres and MySQL, RDS SQL Server uses **Windows Authentication** and requires `license_model = "license-included"`.

**Security group port:** `1433` (not 5432 or 3306).

**DSN format** for go-mssqldb driver:
```
sqlserver://bench:<password>@<host>:1433?database=benchdb
```

Mirror `modules/rds-mysql/` but change port/family/engine and add `license_model = "license-included"`.

## RDS instance class minimums

CDC benches under sustained 50K+ msg/sec require:

| Engine | Minimum instance class | Storage | IOPS |
|--------|------------------------|---------|------|
| Postgres | `db.r6g.2xlarge` | 400 GB gp3 | 12000 |
| MySQL | `db.r6g.2xlarge` | 400 GB gp3 | 12000 |
| SQL Server | `db.r5.2xlarge` (Graviton not available for SQL Server on RDS) | 400 GB gp3 | 12000 |

Smaller instances will appear to work at low CPU points (vCPU=1) and degrade silently as the sweep ramps. The `iops` parameter is **required if `storage_gb >= 400`** and **forbidden if `storage_gb < 400`** for the Postgres engine — see `bench-debugging-history` #28.

## Backup window collisions (false alarm)

An earlier hypothesis blamed RDS auto-backup window collisions for the vCPU-8 mysql degradation pattern. **This was wrong** — pinning `backup_window` to off-hours did not change the pattern. See `bench-debugging-history` #25. Real cause is RDS-internal at sustained ~5min of 150K writes/sec into a single table (gp3 throttling, binlog flush back-pressure, or per-instance IOPS soft ceiling).

When sizing a new CDC bench, do not assume the backup window is the bottleneck. Profile producer-side throughput first.
