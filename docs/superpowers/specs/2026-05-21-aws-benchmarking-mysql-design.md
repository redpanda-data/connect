# AWS benchmarking: mysql_cdc scenario

**Date:** 2026-05-21
**Status:** Design — ready for implementation plan
**Bucket:** 3, item 1 (first proof-point that the framework recipe generalises past postgres)

## Goal

Onboard `mysql_cdc` as the second connector in the AWS benchmarking framework.
Publish a row in `docs/benchmark-results/mysql.md` with median MB/s at 1/2/4/8
vCPU on a sustained-write workload, the same shape as the existing
`postgres/orders-cdc` row.

Why mysql first: it is the closest sibling to postgres (RDS-based, CDC-pattern).
If the recipe doesn't generalise here, it won't anywhere. Other bucket-3
connectors (sqlserver, dynamodb, s3, iceberg, kafka-to-pg) wait until mysql
proves the pattern.

## Non-goals

- No new framework features. matrix/SSM/log-fetch/Prometheus/cost-check/orphan-cleanup paths are connector-agnostic and stay untouched.
- No comparison study (mysql vs postgres throughput). Each row stands on its own.
- No snapshot-mode benchmark. `stream_snapshot: false`; this scenario measures binlog-position-forward CDC under heavy writes, matching the postgres scenario.

## Architecture

Five new pieces and one runner generalisation:

| # | What | Where |
|---|------|-------|
| 1 | RDS MySQL Terraform module | `benchmarking/aws/terraform/modules/rds-mysql/` |
| 2 | MySQL stack | `benchmarking/aws/terraform/stacks/mysql/` |
| 3 | MySQL seeder (`database/sql` + `go-sql-driver/mysql`) | `benchmarking/aws/seeders/cdc-rows-mysql/` |
| 4 | Scenario YAML | `benchmarking/aws/scenarios/mysql/orders-cdc.yaml` |
| 5 | Empty results doc | `docs/benchmark-results/mysql.md` |
| 6 | Engine abstraction in runner | `benchmarking/aws/runner/scenario.go` + 3 sites in `main.go` |

## Runner generalisation (item 6)

The current runner hardcodes Postgres in three places:

```go
// main.go: renderSeedScript
POSTGRES_DSN=%q /opt/bench/%s seed ...

// main.go: combineReset
psql %q -v ON_ERROR_STOP=1 -c %q

// main.go: renderWorkloadScript
POSTGRES_DSN=%q /opt/bench/cdc-rows workload ...
```

Replace with a connector-keyed engine spec in `scenario.go`:

```go
type engineSpec struct {
    DSNOutputKey string // terraform output key
    DSNEnvVar    string // env var name in seed/workload scripts
    // For reset CLI, we ship discrete pieces (host/port/user/pass/db) so the
    // reset shell line stays readable. The DSN-as-URL form that psql accepts
    // is awkward for the mysql CLI.
    ResetHostOutputKey string
    ResetPortOutputKey string
    ResetUserOutputKey string
    ResetPassOutputKey string
    ResetDBOutputKey   string
    ResetCLITemplate   string // formatted with: host, port, user, pass, db, sql
}

var engineSpecs = map[string]engineSpec{
    "postgres_cdc": {
        DSNOutputKey:     "postgres_dsn",
        DSNEnvVar:        "POSTGRES_DSN",
        ResetCLITemplate: `psql %q -v ON_ERROR_STOP=1 -c %q`, // DSN, SQL — keeps existing one-liner shape
    },
    "mysql_cdc": {
        DSNOutputKey:       "mysql_dsn",
        DSNEnvVar:          "MYSQL_DSN",
        ResetHostOutputKey: "mysql_host",
        ResetPortOutputKey: "mysql_port",
        ResetUserOutputKey: "mysql_user",
        ResetPassOutputKey: "mysql_password",
        ResetDBOutputKey:   "mysql_db",
        ResetCLITemplate:   `mysql -h %q -P %q -u %q -p%q %q -e %q`, // host, port, user, pass, db, sql
    },
}
```

`combineReset` looks up the engine spec, builds the prelude (`set -euo pipefail`), and emits one CLI line per reset step. The two engines have different placeholder counts (postgres: DSN+SQL = 2; mysql: host/port/user/pass/db/SQL = 6), so the dispatch is shape-driven: if `ResetHostOutputKey` is set, build the mysql form; otherwise fall back to the postgres DSN form. (The plan may instead split this into two small `buildResetLine` helpers — either is fine, as long as adding a third engine is a one-spec-entry change, not a switch-statement edit.)

`renderSeedScript` and `renderWorkloadScript` look up the engine spec and emit `<EnvVar>=%q /opt/bench/<seeder> ...`.

**Unknown connector** → fail fast in `Scenario.Validate()` with a clear error: `unsupported connector "X"; add an entry to engineSpecs`.

**Runner instance prerequisite:** the load-gen and runner EC2 instances need the `mysql` client installed. Add `dnf install -y mariadb105` to whatever bootstrap path puts `psql` there today (check `terraform/shared/runner.tf`'s user-data or AMI baking; add alongside, do not replace).

## Terraform: rds-mysql module

`benchmarking/aws/terraform/modules/rds-mysql/main.tf` — mirrors `rds-postgres` with engine-specific changes:

```hcl
engine                  = "mysql"
engine_version          = var.engine_version   # default "8.0.46"
backup_retention_period = 1                    # CRITICAL: RDS purges binlog
                                               # immediately if backups are off.
                                               # 1 day is the minimum that keeps
                                               # binlog around long enough for
                                               # CDC to read it.
```

Parameter group family `mysql8.0`, with these defaults in `var.parameters`:

```hcl
default = {
  binlog_format    = "ROW"
  binlog_row_image = "FULL"
  binlog_checksum  = "NONE"   # go-mysql client compat (some versions choke on CRC32)
}
```

`apply_method = "pending-reboot"` on all parameters (same pattern as postgres
module). Combined with `apply_immediately = true` on the instance,
Terraform reboots the instance after parameter group changes.

Other shape (identical to postgres module):
- Security group on port 3306, ingress from `client_sg_ids`.
- gp3 storage, IOPS configurable.
- `random_password` for master.
- `db_subnet_group`, `publicly_accessible = false`, `skip_final_snapshot = true`, `deletion_protection = false`.

### Outputs (5 + 1 sensitive)

```hcl
output "mysql_dsn" {
  # go-sql-driver/mysql DSN: user:pass@tcp(host:port)/db?params
  # parseTime=true makes DATETIME → time.Time
  # tls=skip-verify because RDS-internal CA isn't in the runner image
  value     = "${var.master_username}:${random_password.master.result}@tcp(${aws_db_instance.this.address}:3306)/${var.db_name}?parseTime=true&tls=skip-verify"
  sensitive = true
}
output "mysql_host"     { value = aws_db_instance.this.address }
output "mysql_port"     { value = 3306 }
output "mysql_user"     { value = var.master_username }
output "mysql_db"       { value = var.db_name }
output "mysql_password" {
  value     = random_password.master.result
  sensitive = true
}
```

### Variables (defaults)

| Variable | Default |
|---|---|
| `engine_version` | `"8.0.46"` |
| `db_name` | `"benchdb"` |
| `master_username` | `"bench"` |
| `instance_class` | `"db.r6g.2xlarge"` |
| `storage_gb` | `400` |
| `iops` | `12000` |

## Terraform: mysql stack

`benchmarking/aws/terraform/stacks/mysql/main.tf` — identical structure to
`stacks/postgres/main.tf`:

- `terraform { backend "s3" {} }`
- Provider with `default_tags { Project="redpanda-connect-bench", Stack="mysql", ManagedBy="terraform" }`
- `data "terraform_remote_state" "shared"` (same bucket/region; key passed via `-backend-config=key=mysql` at init by the runner)
- `module "rds" { source = "../../modules/rds-mysql"; ... }`, SGs wired from shared outputs (`runner_sg_id`, `load_gen_sg_id`)

`outputs.tf` re-exports the 6 mysql_* outputs from the module.

## Seeder: cdc-rows-mysql

`benchmarking/aws/seeders/cdc-rows-mysql/{main.go,sql.go}` — copy + adapt of
the existing `cdc-rows` seeder. Same CLI shape (`seed` / `workload`
subcommands, same flags, same defaults).

**`main.go`:** identical to `cdc-rows/main.go` except the env-var name
reference in the help text (functional path uses `MYSQL_DSN`).

**`sql.go` — diff vs cdc-rows/sql.go:**

| Postgres | MySQL |
|---|---|
| `github.com/jackc/pgx/v5/pgxpool` | `database/sql` + `github.com/go-sql-driver/mysql` |
| `pool.Exec(ctx, stmt, payload)` | `db.ExecContext(ctx, stmt, payload, payload, ...)` (positional) |
| `BIGSERIAL PRIMARY KEY` | `BIGINT AUTO_INCREMENT PRIMARY KEY` |
| `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | `DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)` |
| `(NOW(),$1)` reusable placeholder | `(NOW(6),?)` positional |
| `pgxpool.NewWithConfig` / `MaxConns=8` | `sql.Open("mysql", dsn)` + `db.SetMaxOpenConns(8)` |
| `os.Getenv("POSTGRES_DSN")` | `os.Getenv("MYSQL_DSN")` |

**Positional-placeholder fix:** go-sql-driver requires N args for N `?` marks.
Each tick worker builds `args := make([]any, perTick); for i := range args { args[i] = payload }` and passes `args...` to `ExecContext`. Payload is a string; N references are cheap.

**Worker pattern:** unchanged — 8 workers, `rate/8/10` rows per 100ms tick,
`SetMaxOpenConns(8)` matches.

**License header:** Apache-2.0 / BSL block matching the existing seeder.

**go.mod:** `github.com/go-sql-driver/mysql` should already be a transitive
dep via the mysql_cdc connector. Verify with `grep go-sql-driver/mysql go.mod`; if absent, add via `go mod tidy` after seeder files exist.

## Scenario YAML

`benchmarking/aws/scenarios/mysql/orders-cdc.yaml`:

```yaml
name: mysql-orders-cdc
description: |
  Stream changes from a high-write MySQL orders table (target 80K writes/sec
  ≈ 96 MB/s) so the mysql_cdc input — not the producer — is the bottleneck.
  TRUNCATE between sweep points keeps the table size bounded (no Trap 3).

connector: mysql_cdc
stack: mysql

infra:
  source:
    instance_class: db.r6g.2xlarge
    storage_gb: 400
    iops: 12000
    # parameters left at module defaults: binlog_format=ROW, binlog_row_image=FULL,
    # binlog_checksum=NONE. backup_retention_period=1 is set in the module itself
    # because it's a required-for-CDC constant, not a per-scenario knob.
  runner:
    instance_type: c8g.4xlarge

dataset:
  initial_rows: 0          # TRUNCATE-before-every-point makes a seed pointless;
                           # ensureTable still runs to CREATE the table.
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows-mysql

workload:
  write_rate_per_sec: 80000
  duration: 15m
  warmup: 2m

pipeline:
  cache_resources:
    - label: bench_checkpoint
      memory: {}
  input:
    mysql_cdc:
      flavor: mysql
      dsn: ${MYSQL_DSN}
      tls:
        skip_cert_verify: true   # RDS-internal CA isn't in the runner image;
                                  # mysql_cdc uses NewTLSField (no `enabled` toggle).
      stream_snapshot: false
      tables: [orders]
      checkpoint_cache: bench_checkpoint
      checkpoint_key: mysql_bench
      batching:
        count: 5000
        period: 1s

matrix:
  cpu_points: [1, 2, 4, 8]

reset:
  - sql: "TRUNCATE TABLE orders"
```

### Pipeline-config rendering subtlety

`cache_resources` lives at the Connect stream config root, not nested under
`input`. Verify how the runner emits the Connect config from `pipeline:`:
- If the runner template-substitutes the whole `pipeline:` block into the Connect config, `cache_resources:` under `pipeline:` lands at the right level.
- If the runner expects only an `input:` key under `pipeline:`, scenario.go needs a minor addition to surface `cache_resources` at the right level.

The plan must include a task to check this and fix if needed. This is a known
unknown — easier to verify than to guess.

### Reset SQL behavior

`TRUNCATE TABLE orders` runs before every sweep point. With `initial_rows: 0`,
`ensureTable` (in the seed step that runs once before the sweep) still
DROPs+CREATEs the table, so the first TRUNCATE is operating on an existing
empty table and succeeds.

## Testing strategy

### Unit (no AWS)

- `scenario_test.go`: engine-spec map resolves `postgres_cdc` and `mysql_cdc`; unknown connector returns clear error.
- `main_test.go` (or new render-test file): given a mysql scenario, the seed/reset/workload scripts contain `MYSQL_DSN=` and `mysql -h ... -e`; given a postgres scenario, they contain `POSTGRES_DSN=` and `psql`.

### Validate (no AWS)

- `task aws:validate scenario=mysql/orders-cdc` — runs `Scenario.Validate` + `terraform validate` on the new stack. Must pass before any AWS spend.
- If `task aws:validate` doesn't currently walk all stacks for terraform-validate, generalise it.

### Live AWS smoke (one run)

- `task aws:bench scenario=mysql/orders-cdc` under `aws-vault exec ...` with `REDPANDA_LICENSE_FILEPATH=` set.
- **Success:** all 4 sweep points produce non-zero samples; a row appears in `docs/benchmark-results/mysql.md`; SUMMARY.md auto-refreshes.
- **Expected cost:** ~$3, ~90 min.
- **Acceptable retries:** 1–2 to shake out scenario sizing (per bench-debugging-history pattern — every new connector hits a few env-coupling issues).

## Operator surface

The standard run command becomes:

```
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws && \
  unset AWS_PROFILE && \
  aws-vault exec AWSAdministratorAccess-605419575229 -- \
    env REDPANDA_LICENSE_FILEPATH=/Users/prakhar.garg/Documents/connect_prakhar/rpcn.license \
    task aws:bench scenario=mysql/orders-cdc
```

Identical workflow discipline applies (aws-vault wrap, license at repo root,
SIGINT not SIGKILL).

README updates:
- `benchmarking/aws/README.md` — add a "MySQL" section mirroring the existing Postgres section. Document `backup_retention_period=1` gotcha. Document the engine-spec extension pattern for future connectors.

## Open risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| `mysql_cdc` 1-vCPU ceiling > 96 MB/s → workload-bound (Trap 1) | Medium | First-run signal; bump `write_rate_per_sec` to 150K on retry. ~$3 to discover. |
| `backup_retention_period=1` triggers a daily backup window that perturbs the 90-min bench | Low | RDS backup windows are configurable; if it bites, set `backup_window` to a fixed daily slot outside the bench's typical run time. |
| Pipeline-config rendering doesn't surface `cache_resources` correctly | Medium | Plan includes a verification task; fix is small (template tweak). |
| `mysql` client missing on runner EC2 → reset script fails at first point | Medium | Plan includes `dnf install -y mariadb105` in the runner bootstrap. |
| Seeder positional-placeholder bug (forgot to expand args to N copies) | Low | Caught by `task aws:validate` running a small dry-run? No — validate doesn't seed. Caught by the first AWS smoke. |

## Estimated effort and cost

- **Implementation:** mirrors bucket-2-orphan-cleanup-lambda size. ~10–12 commits via subagent-driven-development.
- **AWS spend:** ~$3 first smoke + ~$3 budget for one retry if scenario sizing needs adjustment = **~$6 expected**.
- **Wall-clock:** ~2 hours of focused work + 1.5 hours of AWS run time.

## Plan ordering

1. This spec.
2. Implementation plan at `docs/superpowers/plans/2026-05-21-aws-benchmarking-mysql.md` via the writing-plans skill.
3. Execute via subagent-driven-development.
4. Live AWS smoke: verify all 4 sweep points produce real numbers.
5. Publish row in `docs/benchmark-results/mysql.md`; SUMMARY.md auto-refresh picks it up.
6. Update memory: mark bucket-3 item 1 done, note any new gotchas in [[bench-debugging-history]] and [[bench-rds-quirks]].
