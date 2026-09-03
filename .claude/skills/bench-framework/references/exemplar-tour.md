# Exemplar tour: postgres_cdc + mysql_cdc

The two reference benches. Read these in order when adding a new CDC connector — the new artifacts mirror these.

## TF stack: postgres

`benchmarking/aws/terraform/stacks/postgres/`

| File | Purpose |
|------|---------|
| `main.tf` | Composes shared (via `terraform_remote_state`) with `modules/rds-postgres` |
| `variables.tf` | Inputs: `region`, `runner_instance_type`, `bench_session_id`, RDS instance class, storage, iops, parameter overrides |
| `outputs.tf` | `postgres_dsn` (the value `engineSpec["postgres_cdc"].DSNOutputKey` reads), plus `postgres_host/port/user/password/db` for KC props rendering |

## TF stack: mysql

`benchmarking/aws/terraform/stacks/mysql/`

Same shape as postgres, but uses `modules/rds-mysql`. Outputs: `mysql_dsn` + discrete `mysql_host/port/user/password/db` (engineSpec uses the discrete ones for `mysql` CLI reset — unlike psql, the mysql client wants `-h/-P/-u/-p`, not a DSN URL).

## Scenarios

`benchmarking/aws/scenarios/postgres/orders-cdc.yaml` and `benchmarking/aws/scenarios/mysql/orders-cdc.yaml`.

Both share:
- `initial_rows: 0`
- `TRUNCATE TABLE orders` in reset
- `write_rate_per_sec: 150000` (probes the source-side ceiling)
- `cpu_points: [1, 2, 4, 8]`
- Runner `c8g.4xlarge`, RDS `db.r6g.2xlarge` + 400 GB gp3 + 12000 IOPS

Postgres-specific:
- `tls: { skip_cert_verify: true }` (NO `enabled:`)
- `slot_name: bench_slot` (Connect side)
- Reset drops `bench_slot` AND `kc_bench_slot` (both engines' slots) before TRUNCATE

MySQL-specific:
- No slot in input config (binlog doesn't need named slots)
- Reset is just TRUNCATE

## engineSpec entries

`benchmarking/aws/runner/scenario.go::engineSpecs`. Map keyed by connector name. Two patterns:

**Postgres pattern (DSN-only reset):**
```go
"postgres_cdc": {
    DSNOutputKey: "postgres_dsn",
    DSNEnvVar:    "POSTGRES_DSN",
},
```
The reset script calls `psql "$POSTGRES_DSN"` — discrete fields not needed because `psql` accepts a DSN URL.

**MySQL pattern (discrete-fields reset):**
```go
"mysql_cdc": {
    DSNOutputKey:       "mysql_dsn",
    DSNEnvVar:          "MYSQL_DSN",
    ResetHostOutputKey: "mysql_host",
    ResetPortOutputKey: "mysql_port",
    ResetUserOutputKey: "mysql_user",
    ResetPassOutputKey: "mysql_password",
    ResetDBOutputKey:   "mysql_db",
},
```
The reset script calls `mysql -h $H -P $P -u $U -p$PW $DB -e "..."`. The `mysql` CLI doesn't accept a DSN URL.

**Decision rule for a new connector:** if your engine's CLI accepts a DSN/connection-string URL, use the postgres pattern (just `DSNOutputKey` + `DSNEnvVar`). Otherwise use the mysql pattern with discrete fields.

## kcConnectorSpec entries

`benchmarking/aws/runner/kcconnectors.go::kcConnectorSpecs`. Map keyed by the SAME name as `engineSpecs`. Each entry: `Class`, `PropsTemplate` (text/template-rendered JSON), `Direction` (`kcSource` or `kcSink`), `RequiredPlugins` glob list.

The `PropsTemplate` is the most fiddly part. Inputs: `{{.Host}}`, `{{.Port}}`, `{{.User}}`, `{{.Password}}`, `{{.Database}}`, `{{.TopicPrefix}}`, `{{.SchemaTables}}`, plus connector-specific bits (`{{.BootstrapServers}}` for MySQL schema history).

When writing a new entry:
1. Copy the postgres or mysql entry as a base.
2. Find the Debezium docs for your connector and adjust the JSON keys.
3. **Critical:** set `snapshot.mode` according to whether your engine needs an offset or can stream from current position. See [kc-connector-mapping.md](kc-connector-mapping.md#kcconnectorspec-template-gotchas).
4. Set `RequiredPlugins` to match the installed plugin glob (e.g. `debezium-connector-postgres*`).

## Reset blocks

In each scenario YAML's `reset:` array. Two patterns:

**Postgres:**
```yaml
reset:
  - sql: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"
  - sql: "SELECT pg_drop_replication_slot('kc_bench_slot') FROM pg_replication_slots WHERE slot_name='kc_bench_slot'"
  - sql: "TRUNCATE TABLE orders"
```

**MySQL:**
```yaml
reset:
  - sql: "TRUNCATE TABLE orders"
```

The reset runs BEFORE EVERY sweep point (both engines). `combineReset` in `runner/scripts.go` also auto-injects KC connector DELETE + topic deletes when KC is in the engine list.
