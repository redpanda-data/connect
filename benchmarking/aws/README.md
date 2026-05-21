# AWS Benchmarking Framework

Production-shaped benchmarks for Redpanda Connect connectors, run on real AWS infrastructure.

The design lives at [`docs/superpowers/specs/2026-05-19-aws-benchmarking-framework-design.md`](../../docs/superpowers/specs/2026-05-19-aws-benchmarking-framework-design.md). The implementation plan that built this folder lives at [`docs/superpowers/plans/2026-05-19-aws-benchmarking-framework-foundation.md`](../../docs/superpowers/plans/2026-05-19-aws-benchmarking-framework-foundation.md).

---

## Table of contents

1. [What this is](#what-this-is)
2. [Status](#status)
3. [First-time setup](#first-time-setup)
4. [Running a benchmark](#running-a-benchmark)
5. [What happens during a run](#what-happens-during-a-run)
6. [Adding a new scenario](#adding-a-new-scenario)
7. [Adding a new connector](#adding-a-new-connector)
8. [Known limitations](#known-limitations)
9. [Troubleshooting](#troubleshooting)
10. [Cost](#cost)
11. [Orphan cleanup](#orphan-cleanup)
12. [File reference](#file-reference)

---

## What this is

A connector-agnostic framework that turns a scenario YAML file into:

- An AWS environment (VPC, runner EC2, load-gen EC2, source — e.g. RDS Postgres — and a results S3 bucket) via Terraform.
- A built-from-current-branch `redpanda-connect` binary staged onto the runner host.
- A seeded dataset on the source.
- A CPU sweep across `[1, 2, 4, 8]` vCPU points, each running for `warmup + duration` (default 2 min + 15 min) with Connect pinned to a dedicated cpuset.
- A JSON results file + an appended `## AWS — …` section in `docs/benchmark-results/<connector>.md`.
- An automatic teardown.

It runs as a single command from your laptop. Nothing CI-driven yet.

## Status

| Layer                              | Status |
|------------------------------------|---|
| Scenario parser + validator        | ✅ working, 21+ unit tests |
| Stats parser + percentiles + anomalies | ✅ working, unit tested |
| Runner orchestrator (CLI, terraform wrapper, SSM, matrix, render) | ✅ working |
| Terraform shared stack (VPC/IAM/EC2/S3) | ✅ working on AWS |
| Terraform postgres stack (RDS)     | ✅ working on AWS |
| `cdc-rows` seeder (Postgres path)  | ✅ working — sustains ~108K rows/sec into RDS db.r6g.2xlarge |
| End-to-end through stage 5 (seed)  | ✅ verified on real AWS |
| Connect + license boot on runner   | ✅ verified |
| Postgres CDC TLS connection        | ✅ verified |
| **CPU sweep sample capture**       | ⚠ partial — see [Known limitations](#known-limitations) |
| Destroy on success or SIGINT       | ✅ working — no orphans observed |
| Other connectors (mysql, sqlserver, dynamodb, s3, iceberg) | 🔜 future plans — framework supports them |

The framework foundation runs end-to-end on AWS, but a known SSM output-truncation issue prevents publishing reliable percentile data today. See [Known limitations](#known-limitations) for the path forward.

---

## First-time setup

You only do these once per laptop / per AWS account.

### 1. Install tools

```bash
# macOS via Homebrew:
brew install go terraform jq go-task
brew install --cask aws-vault            # for stable AWS session management
# AWS CLI v2: download from https://aws.amazon.com/cli or via brew install awscli
```

Versions used during initial development: Go 1.22+, Terraform 1.15+, AWS CLI 2.33+, go-task 3.48+, aws-vault 7.2+.

### 2. AWS account + SSO

Use a **dedicated AWS account** for benchmarking. The framework provisions and destroys resources frequently and will create/destroy VPCs, EC2s, RDS instances, S3 buckets, IAM roles.

Configure an SSO profile in `~/.aws/config`. Example, matching the setup this framework was built against:

```ini
[profile bench]
sso_session     = bench-sso
sso_account_id  = 605419575229
sso_role_name   = AWSAdministratorAccess
region          = us-east-2
output          = json

[sso-session bench-sso]
sso_start_url            = https://redpanda-data.awsapps.com/start
sso_region               = us-east-2
sso_registration_scopes  = sso:account:access
```

Then `aws sso login --profile bench` opens a browser; once it confirms, you have SSO credentials cached.

### 3. Wrap with aws-vault

A bench run takes ~90 min. Plain SSO sessions are typically configured to expire in under an hour, which means the AWS SDK calls the runner makes will start failing partway through a run. **Always run benches through aws-vault** — it mints a 12 h STS session up front, so the runner's SDK calls keep working across the entire run.

Confirm aws-vault sees your profile and can mint a session:

```bash
aws-vault list                                          # should show your profile
aws-vault exec bench -- aws sts get-caller-identity     # should print your identity
aws-vault exec bench -- env | grep AWS_CREDENTIAL_EXPIRATION   # should show ~12h in future
```

### 4. Bootstrap the Terraform state backend

A one-time bootstrap in the bench AWS account. The framework's shared backend is hard-coded in `terraform/backend.hcl`; if you're running in a different account, edit those names in a fork.

```bash
aws-vault exec bench -- aws s3api create-bucket \
  --bucket redpanda-connect-bench-tfstate \
  --region us-east-2 \
  --create-bucket-configuration LocationConstraint=us-east-2

aws-vault exec bench -- aws s3api put-bucket-versioning \
  --bucket redpanda-connect-bench-tfstate \
  --versioning-configuration Status=Enabled

aws-vault exec bench -- aws s3api put-bucket-encryption \
  --bucket redpanda-connect-bench-tfstate \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws-vault exec bench -- aws s3api put-public-access-block \
  --bucket redpanda-connect-bench-tfstate \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws-vault exec bench -- aws dynamodb create-table \
  --table-name redpanda-connect-bench-tflocks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-2
```

### 5. Redpanda Enterprise license

Most useful scenarios exercise enterprise connectors (`postgres_cdc`, `mysql_cdc`, …). Without a license Connect refuses to start the input.

Get a license file from your Redpanda account (or via a sandbox/trial). Place it **outside macOS's TCC-protected directories** (`Downloads`, `Documents`, `Desktop`, etc.) — those directories block file reads from sandboxed processes like the runner. Putting it at the repo root works (the repo is gitignored to keep `*.license` out):

```bash
mv ~/Downloads/your.license /Users/yourname/connect_repo/rpcn.license
chmod 0600 /Users/yourname/connect_repo/rpcn.license
```

Then point the runner at it:

```bash
export REDPANDA_LICENSE_FILEPATH=/Users/yourname/connect_repo/rpcn.license
```

Or pass `--license-file=<path>` on the runner command line.

---

## Running a benchmark

### The one command

From inside `benchmarking/aws/`:

```bash
aws-vault exec bench -- \
  env REDPANDA_LICENSE_FILEPATH=$PWD/../../rpcn.license \
  task aws:bench scenario=postgres/orders-cdc
```

Adjust the license path to match where you put your license file.

This runs end-to-end: `terraform apply` → seed → CPU sweep → write JSON + append markdown → `terraform destroy`. Expect ~90 min wall-clock and ~$2–5 in AWS spend.

### Variants

```bash
# Provision once, keep infra for iteration. Useful for rerunning the sweep
# without ~9 min of re-provisioning each time.
aws-vault exec bench -- task aws:bench scenario=postgres/orders-cdc keep=true

# Tear down explicitly later
aws-vault exec bench -- task aws:down scenario=postgres/orders-cdc

# Keep infra if the run errors (default is destroy-on-failure)
aws-vault exec bench -- task aws:bench scenario=postgres/orders-cdc keep-on-fail=true

# Validate the YAML + the rendered config without spending money
task aws:validate scenario=postgres/orders-cdc

# Regenerate the AWS Bench Results section in SUMMARY.md
task aws:summary
```

The "AWS Bench Results" section in `docs/benchmark-results/SUMMARY.md` is auto-refreshed at the end of every `task aws:bench`. If you hand-edit the file or want to manually regen after adding results, use `task aws:summary`. This walks `benchmarking/aws/results/<connector>/<scenario>/*.json`, picks the latest run per scenario, and rewrites the section between the `<!-- bench:aws:start -->` / `<!-- bench:aws:end -->` markers.

### What a healthy run looks like

```
[1/7] loaded scenario postgres-orders-cdc
[2/7] terraform apply (shared + stack) complete
[3/7] built redpanda-connect
[stage] download: s3://…/stage/redpanda-connect to /opt/bench/redpanda-connect
[stage] download: s3://…/stage/config.yaml to /opt/bench/config.yaml
[stage] download: s3://…/stage/license.jwt to /opt/bench/license.jwt
[4/7] staged binary + config on runner
[seed] seeded 75000000 rows into orders in 11m26.918673831s
[5/7] seed complete
=== sweep point: 1 vCPU (warmup 2m0s, window 15m0s) ===
[reset]  pg_drop_replication_slot
[reset] --------------------------
[reset] (0 rows)
[bench] starting bench: 1 vCPU, 2 GiB, warmup 120s, window 900s
[bench] level=info msg="Successfully loaded Redpanda license" license_type=enterprise
[bench] level=info msg="Input type postgres_cdc is now active"
[bench] level=info msg="rolling stats: …"
…
  -> median … MB/s (p5 …, p95 …, peak …), N anomalies
=== sweep point: 2 vCPU (warmup 2m0s, window 15m0s) ===
…
[6/7] sweep complete
[7/7] terraform destroy
Destroy complete! Resources: 5 destroyed.
Destroy complete! Resources: 20 destroyed.

✓ done — JSON: benchmarking/aws/results/postgres/orders-cdc/<timestamp>.json
           md: docs/benchmark-results/postgres.md (appended)
           summary: docs/benchmark-results/SUMMARY.md (regenerated)
```

The whole thing usually finishes in ~90 min.

### Per-stage timings (observed)

| Stage | Typical |
|---|---|
| Terraform apply shared | ~2 min |
| Terraform apply postgres (RDS provision) | ~6 min |
| Build redpanda-connect for linux/arm64 | ~30 s |
| Stage 415 MB binary to runner via S3+SSM | ~30 s |
| Seed 75M × 1.2 KB rows into RDS | ~11.5 min |
| One sweep point (warmup + window) | 17 min |
| Four sweep points | 68 min |
| Terraform destroy | ~5 min |
| **Total** | **~95 min** |

---

## What happens during a run

Architecture is three layers — Terraform, the Go runner orchestrator, and the scenario YAML.

```
┌──────────────────────────────────────────────────────────────────┐
│  Your laptop                                                     │
│                                                                  │
│   aws-vault exec bench -- task aws:bench scenario=…              │
│              │                                                   │
│              ▼                                                   │
│   benchmarking/aws/runner  ◀── current branch built fresh        │
│   • LoadScenario / Validate                                      │
│   • Terraform Init+Apply (shared, then postgres)                 │
│   • Build redpanda-connect for linux/arm64                       │
│   • Upload binary + rendered config + license.jwt to S3          │
│   • SSM-cp those onto the runner EC2                             │
│   • SSM-run the cdc-rows seeder on the load-gen EC2              │
│   • For n in [1, 2, 4, 8]:                                       │
│       reset (drop CDC slot via psql)                             │
│       start workload writer on load-gen (cdc-rows workload)      │
│       SSM-run bench script on runner                             │
│         taskset -c 2-… GOMAXPROCS=n GOMEMLIMIT=… redpanda-connect│
│         sleep warmup+duration                                    │
│         SIGTERM redpanda-connect                                 │
│       parse rolling-stats lines, compute p5/p50/p95/peak         │
│   • Write JSON + render markdown                                 │
│   • Terraform Destroy (deferred, always runs except on keep=true)│
└──────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  AWS (us-east-2)                                                 │
│                                                                  │
│   VPC 10.42/16, 2 AZs, public+private subnets                    │
│   ┌────────────────┐    ┌────────────────┐   ┌────────────────┐  │
│   │ runner EC2     │    │ load-gen EC2   │   │ RDS Postgres   │  │
│   │ c8g.4xlarge    │    │ c8g.large      │   │ db.r6g.2xlarge │  │
│   │ /opt/bench/    │    │ /opt/bench/    │   │ logical repl   │  │
│   │   - connect    │    │   - cdc-rows   │   │ 400 GB gp3     │  │
│   │   - config.yaml│    │ writes via DSN │   │                │  │
│   │   - license.jwt│    └────────────────┘   └────────────────┘  │
│   │ reads via DSN  │                                             │
│   └────────────────┘                                             │
│   S3 bucket rpcn-bench-results-… (staging + raw result JSON)     │
└──────────────────────────────────────────────────────────────────┘
```

Reservation rules:

- Runner EC2 + load-gen EC2 share an instance profile granting SSM, S3 PutObject/GetObject, and Secrets Manager read.
- Postgres RDS is in private subnets; only the runner and load-gen security groups can reach 5432.
- Cores 0,1 on the runner are always reserved for the kernel / SSM agent / metrics scraper; the measured cpuset starts at core 2.
- `GOMEMLIMIT` defaults to `2 × GOMAXPROCS GiB`, configurable in the scenario via `matrix.go_mem_limit_per_vcpu`.

---

## Adding a new scenario

Three things are needed:

1. A scenario YAML under `scenarios/<connector>/<name>.yaml`.
2. A Terraform stack for the connector (already exists for postgres).
3. A seeder if your dataset shape isn't covered by an existing one (`cdc-rows` covers SQL CDC).

### Scenario YAML schema

```yaml
name: postgres-orders-cdc         # used as the result heading
description: |
  One-paragraph customer story. Renders into the markdown
  section produced after the run.

connector: postgres_cdc           # the Connect input you're benching
stack: postgres                   # → terraform/stacks/<stack>

infra:
  source:                         # passed as -var to the stack
    instance_class: db.r6g.2xlarge
    storage_gb: 400
    iops: 12000
    parameters:                   # nested map → JSON-encoded for terraform
      rds.logical_replication: "1"
      max_wal_senders: "20"
  runner:
    instance_type: c8g.4xlarge    # must be ≥ 2 + max(cpu_points) vCPU

dataset:
  initial_rows: 75000000
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows                # binary at benchmarking/aws/seeders/<seeder>

workload:                         # sustained writes during the bench
  write_rate_per_sec: 5000
  duration: 15m                   # framework minimum; rejected if shorter
  warmup: 2m                      # framework minimum

# OR omit `workload:` for bounded-dataset scenarios (snapshot, batch read).
# In that case add to `dataset:`:
#   expected_peak_mb_s: 100       # used to verify wall-clock estimate ≥ 15 min

pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}        # placeholder filled from terraform outputs
      tls:
        skip_cert_verify: true    # see "TLS gotcha" below
      stream_snapshot: false
      schema: public
      tables: [orders]
      slot_name: bench_slot
      batching:
        count: 5000
        period: 1s

matrix:
  cpu_points: [1, 2, 4, 8]        # strictly ascending, all positive
  # overrides:                    # optional per-CPU-point parameter tweaks
  #   8:
  #     batching:
  #       count: 10000
  # go_mem_limit_per_vcpu: 2      # default 2; sets GOMEMLIMIT=N*vcpu GiB

reset:                            # runs between sweep points, on the runner
  - sql: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"
  # - bash: "any shell, ${PLACEHOLDERS} from terraform outputs"
```

Framework-injected fields (do **not** set these in the scenario — they're added by the runner):

```yaml
output:
  processors:
    - benchmark: { interval: 1s, count_bytes: true }
  drop: {}
http: { debug_endpoints: true }
metrics: { prometheus: { add_process_metrics: true, add_go_metrics: true } }
logger: { level: INFO }
```

### Placeholder substitution

Strings of the form `${POSTGRES_DSN}` in `pipeline:` are replaced at runtime by Terraform outputs. The available placeholders match the stack's `outputs.tf`:

| Placeholder | Origin |
|---|---|
| `${POSTGRES_DSN}` | postgres stack |
| `${POSTGRES_ENDPOINT}` | postgres stack |
| `${POSTGRES_PASSWORD}` | postgres stack |
| `${RUNNER_INSTANCE_ID}` | shared stack |
| `${LOAD_GEN_INSTANCE_ID}` | shared stack |
| `${RESULTS_BUCKET}` | shared stack |
| `${VPC_ID}` | shared stack |

### Validate before spending money

```bash
task aws:validate scenario=<connector>/<name>
```

Validates the scenario YAML, instance-type vCPU math, duration minimums, and bounded-dataset wall-clock estimates. No AWS calls.

---

## Adding a new connector

A new connector is a small PR with:

1. **Terraform module** under `terraform/modules/<source-type>/` if the source isn't already supported. Reuse existing modules (`rds-postgres`, `rds-mysql`, etc.) when possible.
2. **Terraform stack** under `terraform/stacks/<connector>/` composing `shared/` and the source module. Use `terraform_remote_state` to read shared outputs (VPC, SG IDs).
3. **Seeder** under `seeders/<name>/` if the SQL flavor differs from what's already shipped (`cdc-rows` for postgres, `cdc-rows-mysql` for mysql).
4. **Scenarios** under `scenarios/<connector>/`.
5. **Engine spec entry** in `runner/scenario.go` — add a one-line map entry under `engineSpecs` keyed by connector name, naming the terraform output keys the runner should read (DSN, host/port/user/pass/db for the reset CLI). The seed/reset/workload script renderers in `runner/scripts.go` pick this up automatically.

---

## MySQL (`mysql_cdc`)

**Scenario:** `mysql/orders-cdc` — 80K writes/sec sustained workload, 4-point vCPU sweep, TRUNCATE between points.

**Run:**

```
cd benchmarking/aws && \
  unset AWS_PROFILE && \
  aws-vault exec AWSAdministratorAccess-605419575229 -- \
    env REDPANDA_LICENSE_FILEPATH=/path/to/rpcn.license \
    task aws:bench scenario=mysql/orders-cdc
```

**RDS-MySQL gotchas:**

- `backup_retention_period = 1` is required. RDS purges binlog immediately if backups are off; `mysql_cdc` then has nothing to read. The `rds-mysql` module hardcodes this — do NOT set it to 0.
- `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_checksum=NONE` are pre-set in the module's parameter group. Override via `var.parameters` if needed.
- `mysql_cdc`'s `tls:` block uses `service.NewTLSField` — there is NO `enabled:` toggle (same gotcha as `postgres_cdc`). Always include `tls: { skip_cert_verify: true }` for RDS.
- The reset CLI uses `mysql` (provided by `mariadb1011` in the runner's user-data). If a future AL2023 image drops that package, `terraform/shared/runner.tf` needs an update.
- `cache_resources` lives at the Connect-config root, not under `input:` — `mysql_cdc` needs a `checkpoint_cache` resource (the scenario uses an in-memory cache so Connect restart between points resets the binlog position automatically).

---

## Known limitations

These are real and worth understanding before you trust numbers from this framework.

### SSM stdout truncation (the big one)

AWS SSM's `GetCommandInvocation` API returns at most **24 KB** of stdout content. Connect emits ~200 KB of `rolling stats:` lines over a 15-min sweep window. The SSM wrapper in `runner/ssm.go` polls every 2s and accumulates new bytes since the last poll, but once total stdout passes 24 KB AWS just stops returning new content — we get exactly one or two samples back out of the ~900 we should have.

**Impact:** percentile data is currently unreliable. The framework runs to completion and reports a number, but that number reflects whichever sample(s) happened to land before the 24 KB cap. Don't publish these numbers as-is.

**Fix (future PR):** change the bench script to write Connect's stdout to a local file on the runner, upload it to S3 at the end of each sweep point, and parse from S3 on the orchestrator side. Roughly 30–60 min of work. Tracked in [Future work](#future-work) below.

### `rolling stats:` line format switches with magnitude

The benchmark processor in Connect emits `rolling stats: X msg/sec, Y B/sec` while throughput is below ~1 MB/s, and switches to `Y MB/sec` above. The runner's regex (`runner/stats.go`) currently requires `MB/sec`, so low-throughput samples — including everything during warmup — are silently dropped.

That's actually fine when paired with the warmup discard, **except** during the SSM truncation it means we may keep the wrong samples. This will be fixed alongside the S3 output path.

### Postgres CDC TLS field shape

`postgres_cdc` uses `service.NewTLSField` (not `NewTLSToggledField`), so its `tls:` block does **not** have an `enabled:` flag. The presence of `tls:` implies TLS. Setting `tls: { enabled: true, … }` causes a config lint error. Set the inner fields directly:

```yaml
tls:
  skip_cert_verify: true        # RDS-internal CA not in the runner image
```

The connector unconditionally overrides the `sslmode=…` from the DSN URL with whatever `tls:` evaluates to, so the `tls:` block is also required for replication to use TLS — RDS rejects unencrypted replication connections.

### RDS Postgres-specific parameter names

`wal_level` is not user-settable on AWS RDS Postgres. Use `rds.logical_replication = "1"` instead — that's the RDS-specific knob that sets `wal_level=logical` under the hood. Already encoded into the scenario's default.

### Engine version churn

`db_engine_version` in the rds-postgres module is currently pinned to `16.14`. RDS retires older patch versions periodically; if `terraform apply` errors with `Cannot find version 16.X for postgres`, bump to a currently-available version. Check with:

```bash
aws-vault exec bench -- aws rds describe-db-engine-versions \
  --engine postgres --region us-east-2 \
  --query "DBEngineVersions[?starts_with(EngineVersion,'16')].EngineVersion" \
  --output text
```

### Plain SSO sessions are too short

Default AWS SSO sessions on the SDK side expire in under an hour. A bench takes ~90 min. **Always wrap your bench in `aws-vault exec`** so the runner gets a 12 h STS session token. Without aws-vault you'll see `InvalidGrantException` / `Token has expired` failures mid-run, and the orchestrator's destroy may also fail.

### macOS TCC blocks Downloads / Documents

Claude Code (and many other sandboxed processes) cannot read files inside `~/Downloads`, `~/Documents`, `~/Desktop`. Put your license file at the repo root (which is gitignored) or in `~/` directly.

### Clock skew

The orchestrator's Go program calls SSM and S3 over the duration of the bench (~90 min). AWS rejects requests whose signature timestamp is more than 15 min skewed from the server-side clock. If your laptop's clock drifts, you'll see `Signature expired` errors, often only in the final `terraform destroy`. Keep your Mac time synced — if it isn't already:

```bash
sudo sntp -sS time.apple.com
```


### Other connectors

Only the `postgres_cdc` scenario ships in this foundation. Modules + stacks for mysql / sqlserver / dynamodb / s3 / iceberg come in follow-up PRs. The framework already supports them — each is roughly: one Terraform module, one stack, optional seeder code path, one or two scenario YAMLs.

---

## Troubleshooting

### "function main is undeclared" when running `go build ./benchmarking/aws/runner`

Run from the **repo root**, not from any subdirectory: `cd /path/to/connect && go build ./benchmarking/aws/runner`.

### "license file …: operation not permitted"

The file is in a TCC-protected macOS directory. Move it out of `Downloads/`, `Documents/`, `Desktop/` etc. The repo root is fine (it's gitignored for `*.license` / `*.jwt`).

### "InvalidGrantException" / "Token has expired" mid-run

Plain SSO session expired. Re-login: `aws sso login --profile bench`, then run the bench wrapped in `aws-vault exec bench --`.

### "Signature expired"

System clock is more than ~15 min off from AWS server time. Sync: `sudo sntp -sS time.apple.com`.

### Bench got most of the way through but stopped — was infra cleaned up?

The runner registers `terraform destroy` as a deferred call **before** the first apply, so it should fire even if the run errors out. To confirm:

```bash
aws-vault exec bench -- aws ec2 describe-instances \
  --region us-east-2 \
  --filters Name=tag:Project,Values=redpanda-connect-bench \
            Name=instance-state-name,Values=running,pending,stopping,stopped \
  --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`Name`].Value|[0]]' --output text

aws-vault exec bench -- aws rds describe-db-instances \
  --region us-east-2 \
  --query 'DBInstances[?DBInstanceIdentifier==`rpcn-bench-pg-pg`].DBInstanceStatus' \
  --output text
```

Both should be empty.

### How do I kill a stuck run?

If the orchestrator goes silent for a long time, you can gracefully signal it so the deferred destroy still runs:

```bash
pgrep -fl 'benchmarking/aws/runner.*bench'    # find the runner pid
kill -INT <runner_pid>                        # SIGINT triggers ctx-cancel → destroy
```

Avoid SIGKILL (`kill -9`) — the deferred destroy won't fire and you'll have to clean up by hand.

### Manual cleanup as a last resort

If the deferred destroy never ran, terraform state still knows what was created. From the postgres stack:

```bash
cd terraform/stacks/postgres
aws-vault exec bench -- terraform destroy -auto-approve \
  -var instance_class=db.r6g.2xlarge \
  -var storage_gb=400 \
  -var iops=12000 \
  -var 'parameters={"rds.logical_replication"="1","max_wal_senders"="20"}'
```

Then the shared stack — the `bench_session_id` is required, find it from current state:

```bash
cd ../../shared
aws-vault exec bench -- terraform state show aws_instance.runner | \
  grep -oE '"bench-session-id" *= *"[^"]*"' | head -1
```

Then destroy with that tag:

```bash
aws-vault exec bench -- terraform destroy -auto-approve \
  -var region=us-east-2 \
  -var runner_instance_type=c8g.4xlarge \
  -var bench_session_id=<the-tag-value-from-above>
```

---

## Cost

Rough estimate for a single-scenario run (us-east-2):

| Resource | Hours running | Approx cost |
|---|---|---|
| Runner EC2 c8g.4xlarge | ~1.6 h | ~$0.95 |
| Load-gen EC2 c8g.large | ~1.6 h | ~$0.13 |
| RDS db.r6g.2xlarge | ~1.5 h | ~$0.85 |
| RDS gp3 storage 400 GB | partial day | ~$0.20 |
| S3 + DynamoDB lock | tiny | < $0.01 |
| Data transfer (intra-AZ) | a few GB | < $0.10 |

**~$2–3 per run.** Re-running with `keep=true` (skip provision/teardown) cuts that roughly in half. Failed runs that get part-way through still incur most of the cost because RDS provision is the long pole.

### Checking spend

```bash
aws-vault exec AWSAdministratorAccess-605419575229 -- task aws:cost-check
```

Prints today's spend, last 7 days, and month-to-date totals filtered on `Project=redpanda-connect-bench`, plus a per-usage-type breakdown. Cost Explorer lags 24–48 hours, so a bench you just ran won't show until tomorrow.

### Manual tag-based audit (alternative)

If you need raw numbers without aws-vault overhead:

```bash
aws-vault exec bench -- aws ce get-cost-and-usage \
  --time-period Start=$(date -v-1d +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity DAILY --metrics UnblendedCost \
  --filter '{"Tags":{"Key":"Project","Values":["redpanda-connect-bench"]}}'
```

---

## Orphan cleanup

A Lambda runs every 15 minutes in the shared stack. It finds resources tagged
`Project=redpanda-connect-bench` whose creation time is older than 3 hours
and destroys them via direct AWS API calls in dependency order (RDS → EC2 →
S3 → IAM → VPC family). It publishes an SNS notification only when something
was actually destroyed.

This is a safety net: if the runner is SIGKILLed, your laptop loses creds
mid-run, or `terraform destroy` errors out, the Lambda mops up within a 3-hour
window. A normal ~90-min bench finishes well inside the TTL and is never
touched.

### Override the TTL (rare)

For a deliberately long-running scenario:

```sh
aws lambda update-function-configuration \
  --function-name redpanda-connect-bench-orphan-cleanup \
  --environment Variables="{BENCH_ORPHAN_TTL_HOURS=6,BENCH_ORPHAN_SNS_TOPIC_ARN=$(terraform -chdir=terraform/shared output -raw orphan_cleanup_sns_topic_arn)}"
```

Reset to default by removing the override (or passing `3`). The Lambda re-reads its env vars on every cold start, so the new TTL takes effect on the next scheduled run.

### Subscribe to SNS notifications (optional)

```sh
aws sns subscribe \
  --topic-arn $(terraform -chdir=terraform/shared output -raw orphan_cleanup_sns_topic_arn) \
  --protocol email \
  --notification-endpoint your.email@redpanda.com
```

The Lambda only publishes when at least one resource was destroyed; no spam on no-op runs.

### Building the Lambda

The Lambda zip is built locally and shipped via terraform's `filebase64sha256`. After modifying `benchmarking/aws/cleanup-lambda/*.go`, rebuild before applying:

```sh
make -C benchmarking/aws/cleanup-lambda zip
```

The next `terraform apply` will detect the changed source hash and update the function.

---

## File reference

```
benchmarking/aws/
├── README.md                              ← you are here
├── Taskfile.yml                           ← operator entry points
├── .gitignore                             ← results/*.json, .terraform/, dist/
├── terraform/
│   ├── backend.hcl                        ← shared S3 + DDB-lock backend config
│   ├── shared/                            ← one apply, reused across all stacks
│   │   ├── main.tf                        ← provider, locals, tags
│   │   ├── variables.tf
│   │   ├── vpc.tf                         ← /16 VPC + subnets + IGW + RT
│   │   ├── security.tf                    ← runner + load-gen SGs
│   │   ├── iam.tf                         ← SSM-managed instance role
│   │   ├── runner.tf                      ← runner + load-gen EC2 (AL2023 arm64)
│   │   ├── results_bucket.tf              ← S3 results bucket
│   │   └── outputs.tf
│   ├── modules/
│   │   └── rds-postgres/                  ← reusable RDS Postgres module
│   └── stacks/
│       └── postgres/                      ← composes shared + rds-postgres
├── scenarios/
│   └── postgres/
│       └── orders-cdc.yaml                ← shipped scenario
├── seeders/
│   └── cdc-rows/                          ← Go program: seed + workload subcommands
│       ├── main.go
│       └── sql.go
├── runner/                                ← Go orchestrator
│   ├── main.go                            ← CLI: bench / validate / down / cost-check
│   ├── scenario.go                        ← scenario YAML parse + validate
│   ├── stats.go                           ← rolling-stats line parser + percentiles
│   ├── anomalies.go                       ← ≥60s dip detection
│   ├── render.go                          ← Result JSON + markdown rendering
│   ├── terraform.go                       ← terraform CLI wrapper
│   ├── ssm.go                             ← SSM RunCommand + streaming output
│   ├── matrix.go                          ← CPU sweep loop
│   ├── templates/result.md.tmpl           ← markdown template (embedded)
│   ├── testdata/                          ← unit-test fixtures
│   └── *_test.go                          ← 21+ unit tests
└── results/                               ← raw per-run JSON (gitignored)
    └── .gitkeep
```

---

## Future work

This is a foundation. Concrete follow-ups:

1. **Fix SSM stdout capture via S3** (highest priority). Without this, percentile data isn't trustworthy.
2. **Fix the regex** to also match `B/sec` for low-throughput samples — same PR.
3. **Implement the orphan-cleanup Lambda** (EventBridge → Lambda → terraform destroy on stacks > 24h old).
4. **Add remaining connectors**: mysql, sqlserver, dynamodb, s3, iceberg. Each is ~1 module + 1 stack + 1–2 scenarios + maybe a seeder code path.
5. **Add Prometheus snapshot capture** at the end of each sweep point — the JSON schema already has `prom_snapshot` (currently empty).
6. **`SUMMARY.md` auto-refresh** — programmatically pick the best AWS run per connector for the at-a-glance table.

Each can ship as its own focused PR using the same framework, no rework needed.
