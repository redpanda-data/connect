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
-- 0. RDS creates a SQL Server instance with NO application database: `db_name` is
--    rejected for every sqlserver engine. Create it first, against a MASTER DSN.
CREATE DATABASE benchdb;

-- 1. Enable CDC on the database. MUST be the RDS wrapper, NOT the native
--    sys.sp_cdc_enable_db — that requires sysadmin, which RDS does not grant to
--    the master user. Runs from any database context because it is qualified.
EXEC msdb.dbo.rds_cdc_enable_db 'benchdb';

-- 2. Enable CDC on the target table (db_owner is enough here).
EXEC sys.sp_cdc_enable_table
  @source_schema        = 'dbo',
  @source_name          = 'orders',
  @role_name            = NULL,
  @supports_net_changes = 0;

-- 3. ONLY NOW can the capture job be tuned. rds_cdc_enable_db does NOT create the
--    job's msdb row — that happens when the first table is enabled. Tuning before
--    step 2 fails with "The Change Data Capture job table containing job
--    information for database 'benchdb' cannot be found in the msdb system
--    database."
EXEC sys.sp_cdc_add_job    @job_type = N'capture', @maxtrans = 5000, @maxscans = 100, @continuous = 1, @pollinginterval = 1;
EXEC sys.sp_cdc_change_job @job_type = N'capture', @maxtrans = 5000, @maxscans = 100, @continuous = 1, @pollinginterval = 1;
```

These go in the **first-run seed script**, not the reset block.

**THE CAPTURE JOB IS A CEILING UPSTREAM OF BOTH ENGINES — tune it or you are not benching the connector.** Neither `microsoft_sql_server_cdc` nor Debezium reads the transaction log; both tail the `cdc.<schema>_<table>_CT` change tables, which SQL Server's own capture job fills. That job does `@maxscans` passes of at most `@maxtrans` transactions, then sleeps `@pollinginterval` seconds. At the stock 10 / 500 / 5 that is a hard ceiling of ~1000 transactions/sec no matter how much CPU either engine has.

**TRUNCATE is ILLEGAL on a CDC-enabled table** ("Cannot truncate table ... because it is published for replication or enabled for Change Data Capture"). The reset must be disable-CDC → TRUNCATE → re-enable-CDC. The re-enable is load-bearing beyond cleanup: a fresh capture instance starts at the current LSN, which is the only reason `stream_snapshot: false` is safe. Without it the connector starts from the capture instance's original `start_lsn` and replays every change ever captured.

**Do NOT stop/start the capture job in the reset.** SQL Server Agent handles start/stop asynchronously. If the job is already running, `sp_cdc_stop_job` succeeds and the immediately following `sp_cdc_start_job` is refused with "the job already has a pending request" — leaving the job STOPPED for the whole sweep point. Both engines then report 0 MB/s with no error anywhere. Only ever start it, and prove it is scanning by writing a sentinel row and requiring `sys.fn_cdc_get_max_lsn()` to advance (see `ensureCaptureJobRunning` in `seeders/cdc-rows-mssql/sql.go`). `cdc.change_tables.start_lsn` being non-NULL is NOT sufficient — it shows the job scanned once, not that it is running now.

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

RDS SQL Server requires `license_model = "license-included"` (there is no BYOL path for se/ee via this API) and is **x86-only** — no Graviton instance classes, same as RDS Oracle.

Authentication is ordinary **SQL Server authentication** with the master username/password, exactly like Postgres and MySQL. (An earlier version of this document claimed Windows Authentication; that is only relevant if you deliberately integrate the instance with AD, which the bench does not.)

CDC needs **Standard or Enterprise** edition — it is unavailable on `sqlserver-ex` / `sqlserver-web`, where `rds_cdc_enable_db` errors out.

**Security group port:** `1433` (not 5432 or 3306).

**DSN format** for go-mssqldb driver:
```
sqlserver://bench:<password>@<host>:1433?database=benchdb&encrypt=true&TrustServerCertificate=true
```

`TrustServerCertificate=true` is required: the RDS-internal CA is not in the runner image. Keep `encrypt=true` for fairness — the mssql-jdbc driver Debezium uses defaults to encryption, so disabling it on the Connect side hands Connect a free CPU saving at every pinned-vCPU point.

A **second DSN pointing at `master`** is also needed, because the application database does not exist yet on a fresh instance (see step 0 above). The bench wires it in via the engineSpec's `ExtraEnvVars` as `MSSQL_MASTER_DSN`.

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
