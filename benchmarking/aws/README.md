# AWS Benchmarking Framework

Production-shaped benchmarks for Redpanda Connect connectors, run on real AWS infrastructure.

This README is the authoritative reference. Design docs and implementation plans that built this framework lived in `docs/superpowers/{specs,plans}/2026-05-*` during development but are no longer in the tree; recover them from git history if needed.

---

## Table of contents

1. [What this is](#what-this-is)
2. [Status](#status)
3. [Using the Claude skill](#using-the-claude-skill)
4. [First-time setup](#first-time-setup)
5. [Running a benchmark](#running-a-benchmark)
6. [What happens during a run](#what-happens-during-a-run)
7. [Adding a new scenario](#adding-a-new-scenario)
8. [Adding a new connector](#adding-a-new-connector)
9. [Known limitations](#known-limitations)
10. [Troubleshooting](#troubleshooting)
11. [Cost](#cost)
12. [Orphan cleanup](#orphan-cleanup)
13. [File reference](#file-reference)

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

Framework:

| Layer | Status |
|-------|--------|
| Scenario parser + validator | ✅ working, unit tested |
| Stats parser + percentiles + anomalies | ✅ working, unit tested |
| Runner orchestrator (CLI, terraform wrapper, SSM, matrix, render) | ✅ working |
| Terraform shared stack (VPC/IAM/EC2/S3) | ✅ working on AWS |
| CPU sweep sample capture + broker-side metrics | ✅ working |
| Destroy on success or SIGINT | ✅ working — no orphans observed |

Connectors (head-to-head Connect vs Kafka Connect unless noted; results in [`docs/benchmark-results/`](../../docs/benchmark-results)):

| Connector | Seeder | Stack | Status |
|-----------|--------|-------|--------|
| `postgres_cdc` | `cdc-rows-postgres` | `rds-postgres` | ✅ full sweep |
| `mysql_cdc` | `cdc-rows-mysql` | `rds-mysql` | ✅ full sweep |
| `oracledb_cdc` | `cdc-rows-oracle` | `rds-oracle` | ✅ full sweep — LogMiner-bound ceiling, see [`oracle.md`](../../docs/benchmark-results/oracle.md) |
| `aws_dynamodb_cdc` | `cdc-ddb` | `dynamodb-bench` | ✅ full sweep (Connect-only — no Debezium DynamoDB comparator in the harness) |
| iceberg (sink) | `json-orders` + `iceberg-tablegen` | `iceberg` (Glue) | ✅ sink sweep |

---

## Using the Claude skill

This repo ships a Claude Code skill at [`.claude/skills/bench-framework/`](../../.claude/skills/bench-framework/) that walks you through the framework interactively — the same flows this README documents, but conversational and context-aware. Use it instead of grepping the README when you can.

### What the skill does

Three flows:

1. **Adding a new connector bench** — interview-driven walkthrough that produces the 5 artifacts (TF stack, scenario YAML, `engineSpec` entry, `kcConnectorSpec` entry, reset SQL). Renders templates from `assets/templates/` and delegates Go edits to the `godev` agent.
2. **Operating an existing bench** — pre-flight checklist (aws-vault, license placement, branch state, cleanup-Lambda zip), exact run command, live-monitoring guidance (S3 paths, heartbeats, SIGINT-only teardown), and result inspection.
3. **Debugging a failed bench** — symptom-keyed lookup into a playbook of known failure modes (workload silent, KC HTTP 500, empty `broker_series`, RDS DNS flake, orphan-cleanup TTL, etc.). Each entry links to the commit SHA that fixed it.

A 17-row "traps" reference is rendered at the top of the skill so the gotchas surface up front, regardless of which flow you pick.

### How to install

The skill is already in the repo — no separate install. To use it:

1. Install Claude Code (see [claude.ai/code](https://claude.ai/code)).
2. Clone this repo and check out the `benchmarking` branch.
3. Open Claude Code in the repo root. The skill auto-loads when your context mentions `benchmarking/aws/`, or invoke it explicitly with `/bench-framework`.

### How the skill is structured

```
.claude/skills/bench-framework/
├── SKILL.md                        ← Top-level decision tree + 3-branch workflow
├── references/                     ← Deep dives loaded on demand
│   ├── traps.md                    ← 17 documented gotchas with anchors
│   ├── bootstrap.md                ← One-time AWS account setup
│   ├── workflow-essentials.md      ← aws-vault, license, SIGINT non-negotiables
│   ├── rds-quirks.md               ← RDS Postgres/MySQL/SQL Server specifics
│   ├── kc-connector-mapping.md     ← Which Kafka Connect plugin to use
│   ├── scenario-sizing.md          ← write_rate, TRUNCATE, observed ceilings
│   ├── exemplar-tour.md            ← File-by-file walk of postgres + mysql exemplars
│   └── debugging-playbook.md       ← Symptom → root cause → fix SHA
└── assets/templates/               ← Scaffolding rendered into your working tree
    ├── tf-stack/                   ← main.tf, variables.tf, outputs.tf templates
    ├── scenario.yaml.tmpl
    ├── engine-spec.go.tmpl
    ├── kc-connector-spec.go.tmpl
    └── reset.sql.tmpl
```

### What the skill does NOT do

- It never executes AWS commands or triggers real spend — that's an operator decision. The skill describes the command and waits for you to run it.
- It doesn't replace this README. It points at the same exemplar files and references the same gotchas — but conversationally. The README is the durable record.

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

> Want a guided walkthrough? Invoke the [`bench-framework` Claude skill](#using-the-claude-skill) — it runs the pre-flight checklist, builds the exact command for your scenario, and steers you to the [Debug](#troubleshooting) section if anything fails.

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
│   • SSM-run the cdc-rows-postgres seeder on the load-gen EC2              │
│   • For n in [1, 2, 4, 8]:                                       │
│       reset (drop CDC slot via psql)                             │
│       start workload writer on load-gen (cdc-rows-postgres workload)      │
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
│   │   - connect    │    │   - cdc-rows-postgres   │   │ 400 GB gp3     │  │
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
3. A seeder if your dataset shape isn't covered by an existing one (`cdc-rows-postgres` covers SQL CDC).

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
  seeder: cdc-rows-postgres                # binary at benchmarking/aws/seeders/<seeder>

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

> The [`bench-framework` Claude skill](#using-the-claude-skill) walks this end-to-end — interview, exemplar selection, template scaffolding, the `godev`/`tester` agent handoffs, and the 1-vCPU smoke. Use it if you'd rather not assemble the artifacts by hand.

A new connector is a small PR with:

1. **Terraform module** under `terraform/modules/<source-type>/` if the source isn't already supported. Reuse existing modules (`rds-postgres`, `rds-mysql`, etc.) when possible.
2. **Terraform stack** under `terraform/stacks/<connector>/` composing `shared/` and the source module. Use `terraform_remote_state` to read shared outputs (VPC, SG IDs).
3. **Seeder** under `seeders/<name>/` if the SQL flavor differs from what's already shipped (`cdc-rows-postgres` for postgres, `cdc-rows-mysql` for mysql).
4. **Scenarios** under `scenarios/<connector>/`.
5. **Engine spec entry** in `runner/scenario.go` — add a one-line map entry under `engineSpecs` keyed by connector name, naming the terraform output keys the runner should read (DSN, host/port/user/pass/db for the reset CLI). The seed/reset/workload script renderers in `runner/scripts.go` pick this up automatically.
6. **KC connector spec entry** in `runner/kcconnectors.go` — needed only if you want the Connect-vs-Kafka-Connect head-to-head comparison. Mirror the postgres or mysql entries.

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
├── README.md                  ← you are here
├── Taskfile.yml               ← operator entry points (aws:bench / validate / down / summary / cost-check)
├── .gitignore                 ← results/*.json, .terraform/, dist/
├── terraform/
│   ├── backend.hcl            ← shared S3 + DDB-lock backend config
│   ├── shared/                ← one apply, reused by every stack: VPC, IAM, EC2
│   │                            runner+load-gen, S3 results, Redpanda brokers,
│   │                            runner cloud-init, orphan-cleanup lambda
│   ├── modules/               ← reusable building blocks
│   │   ├── rds-postgres/ rds-mysql/ rds-oracle/   ← RDS source databases
│   │   ├── dynamodb-bench/                        ← DynamoDB tables + streams
│   │   ├── glue-iceberg/                          ← Glue catalog + S3 warehouse
│   │   └── redpanda/                              ← broker cluster
│   └── stacks/                ← one per connector (composes shared + its module)
│       └── postgres/ mysql/ oracle/ dynamodb/ iceberg/
├── scenarios/                 ← benchmark definitions, one dir per connector
│   └── postgres/ mysql/ oracle/ dynamodb/ iceberg/   *.yaml
├── seeders/                   ← Go programs: seed + workload (+ exec) subcommands
│   ├── cdc-rows-postgres/ cdc-rows-mysql/ cdc-rows-oracle/   ← SQL CDC row writers
│   ├── cdc-ddb/                                              ← DynamoDB item writer
│   ├── json-orders/                                         ← Kafka JSON producer (sink benches)
│   └── iceberg-tablegen/                                    ← pre-creates Iceberg tables
├── runner/                    ← Go orchestrator (see runner/doc.go for the full file map)
│   ├── doc.go                 ← package reading guide (files grouped by concern)
│   ├── main.go                ← CLI: bench / validate / down / cost-check / summary
│   ├── scenario.go            ← scenario schema + engineSpecs (Connect side)
│   ├── kcconnectors.go        ← kcConnectorSpecs (Kafka Connect / Debezium side)
│   ├── matrix.go              ← the CPU sweep
│   ├── …                      ← metrics / topology / render / infra (grouped in doc.go)
│   ├── templates/             ← result + summary markdown templates
│   └── testdata/              ← scenario fixtures for validation tests
├── cleanup-lambda/            ← orphan-cleanup Lambda (destroys stacks past TTL)
└── results/                   ← raw per-run JSON (gitignored)
```

---

## Future work

The framework is mature: broker-side metrics, the CPU sweep, the orphan-cleanup
Lambda, `SUMMARY.md` auto-refresh, and five connectors all ship today. Open
follow-ups:

1. **More connectors** — SQL Server CDC and an S3 sink are the obvious next
   additions. Each is ~1 module + 1 stack + 1–2 scenarios + maybe a seeder
   (see [Adding a new connector](#adding-a-new-connector)).
2. **A Kafka Connect comparator for DynamoDB** — currently Connect-only (no
   Debezium DynamoDB connector in the harness), so its results have no
   head-to-head column.
3. **Lint the rendered Connect config in `validate`** — `validate` checks the
   scenario structure but not the rendered pipeline against the built binary, so
   an unsupported input field only surfaces ~10 min into a run.

## Kafka Connect comparison infra (Plan 1, 2026-05-22)

The shared stack now provisions a 3-broker Redpanda cluster (`im4gn.2xlarge`, NVMe instance store, static IPs in the private subnets) reachable from both `runner` and `load-gen` security groups. Topic/data paths in /var/lib/redpanda/data; admin + Prometheus on `:9644`; Kafka API on `:9092`. The runner EC2 cloud-init now also installs OpenJDK 21, the Apache Kafka 3.8 tarball, Debezium 2.7.3 (Postgres + MySQL), and the Aiven JDBC sink 6.10.0 (Iceberg + Confluent S3 sinks land in Plan 4) to `/opt/kafka-connect/plugins/`. A `kafka-connect.service` systemd unit runs the worker in single-worker distributed mode bound to `bootstrap.servers=<redpanda brokers>` on `:8083`.

For each sweep point, the bench script scrapes `http://<broker0>:9644/public_metrics` every 10s and uploads the framed dump to `s3://<results>/runs/<session>/redpanda-<vcpu>.txt` alongside the existing `prom-<vcpu>.txt`. **No bench numbers change in Plan 1** — Connect's pipeline still writes to `drop`. The broker scrape is captured for Plans 2/3 to use.

Acceptance smoke test (one-time, manual; see Plan 1 task 12):

```
# After `task aws:bench --validate-only` succeeds:
aws ssm start-session --target <runner-id>
$ curl -s localhost:8083/connector-plugins | jq '.[] | .class' | sort -u
$ curl -s http://<broker0>:9644/public_metrics | head -5
```

## Side-by-side: Connect vs Kafka Connect (Plan 2, 2026-05-22)

Each sweep point now runs Connect AND Kafka Connect sequentially at the same vCPU/memory budget against the same workload. Output is two `PointResult` rows per vCPU — one per engine — in the result JSON.

**Run both engines (default):**

```sh
task aws:bench SCENARIO=benchmarking/aws/scenarios/postgres/orders-cdc.yaml
```

**Run only Connect (single-engine regression):**

```sh
go run ./benchmarking/aws/runner bench --scenario=... --engines=connect
```

**How each engine is invoked:**

- **Connect** runs as today: pinned via `taskset` + `chrt --fifo 50` + `GOMEMLIMIT=<N>GiB`, with `output: redpanda` writing to topic `bench_<session>_<connector>_connect`. The top-level `redpanda:` block in the pipeline config carries the broker list.
- **Kafka Connect** stops the cloud-init-started worker, then spawns the JVM directly under `taskset + chrt -Xmx<N>g` so the JVM gets the same CPU/mem budget. The Debezium connector is submitted via `curl PUT /connectors/<name>/config`. After warmup + window, the connector is `DELETE`'d, the JVM is SIGTERM'd, the systemd unit is restarted for the next sweep point.

**Why each engine writes to a separate topic:** broker-side throughput metrics attribute cleanly to one engine via the topic label.

**Reset between engines** (and between sweep points): SQL reset (engine-aware; the existing logic from `scripts.go`), idempotent `DELETE /connectors/bench_<connector>`, and `kafka-topics.sh --delete` for both engines' topics.

## Reporting + cross-engine anomalies (Plan 3, 2026-05-22)

KC's throughput now comes from broker-side metrics (Plan 1 captures
`redpanda-<vcpu>.txt` once per sweep point; Plan 3 parses it). The
markdown report writes one row per `(vcpu, engine)` and a Δ column
showing the per-engine gap; the SUMMARY.md row shows both engines'
medians at the best Connect vCPU. Scenarios where the two engines
diverge by more than 2× at any vCPU get a "Cross-engine divergence"
section listing the offending vCPUs.

**Scheduler/memory fairness fixes:**

- Connect no longer runs under `chrt --fifo 50`. The KC side dropped
  it in Plan 2 (it deadlocked the JVM under single-core taskset);
  Plan 3 drops it from Connect too so both engines run under
  SCHED_OTHER. **Numbers from Plan 1/2 are NOT comparable to Plan 3+
  numbers because of this scheduler change.**
- KC's `-Xmx` is set to `0.75 × memLimitGiB` (floor 1 GiB) so JVM
  overhead (Metaspace, code cache, direct buffers) fits inside the
  memory budget the way Connect's `GOMEMLIMIT` already does. Equal-
  budget runs now have equal-RSS engines.

**Result-JSON shape change:** `PointResult` gains `broker_series`
(per-engine attributed throughput from Redpanda's metrics). `Result`
gains `cross_engine_anomalies` (per-vCPU divergence flags).

**Known limitation:** Plan 2 captures KC's per-sweep-point log to S3 but doesn't parse it. The canonical KC throughput number comes from the broker-side metric introduced in Plan 1 (uploaded to S3 as `redpanda-<vcpu>.txt`), and that's what Plan 3 will surface in reporting. Until Plan 3 lands, the KC `PointResult.Summary` is the zero value (`MedianMBPerSec: 0`) — the data is in S3 but not yet rendered into the markdown.
