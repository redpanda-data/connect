# Bench Framework Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the in-repo `.claude/skills/bench-framework/` skill so any Redpanda engineer can add a new connector bench, run an existing one, or debug a failure without re-learning the framework.

**Architecture:** Single skill with progressive disclosure. `SKILL.md` is a lean decision-tree + checklist (~250 lines). Depth lives in `references/` (loaded on demand). Templates in `assets/templates/` get copied into the working tree with placeholder substitution during scaffolding.

**Tech Stack:** Markdown skill format (Claude Code skills). Source memories live in `/Users/prakhar.garg/.claude/projects/-Users-prakhar-garg-Documents-connect-prakhar/memory/`. References point at existing Go code in `benchmarking/aws/runner/` and exemplar configs in `benchmarking/aws/scenarios/{postgres,mysql}/`.

**Spec:** `docs/superpowers/specs/2026-05-31-bench-framework-skill-design.md`

---

## File map

Files to create (all paths relative to repo root):

```
.claude/skills/bench-framework/
├── SKILL.md
├── references/
│   ├── traps.md
│   ├── rds-quirks.md
│   ├── kc-connector-mapping.md
│   ├── scenario-sizing.md
│   ├── workflow-essentials.md
│   ├── debugging-playbook.md
│   └── exemplar-tour.md
└── assets/
    └── templates/
        ├── tf-stack/
        │   ├── main.tf.tmpl
        │   ├── variables.tf.tmpl
        │   └── outputs.tf.tmpl
        ├── scenario.yaml.tmpl
        ├── engine-spec.go.tmpl
        ├── kc-connector-spec.go.tmpl
        └── reset.sql.tmpl
```

13 files + 1 directory. Each task below produces one file (or a small cluster) and ends with a commit.

---

## Phase A: Scaffolding + foundation

### Task 1: Create skill directory + minimal SKILL.md skeleton

**Files:**
- Create: `.claude/skills/bench-framework/SKILL.md`

This task creates the skill's home and the bare frontmatter. No content beyond the frontmatter — later tasks fill in the body.

- [ ] **Step 1: Create the directory tree**

```bash
mkdir -p .claude/skills/bench-framework/references
mkdir -p .claude/skills/bench-framework/assets/templates/tf-stack
```

- [ ] **Step 2: Write `.claude/skills/bench-framework/SKILL.md`**

```markdown
---
name: bench-framework
description: Use when working in benchmarking/aws/ on the Redpanda Connect benchmarking framework — adding a new connector bench (TF stack + scenario + engineSpec + kcConnectorSpec), running an existing bench, or debugging a bench failure. Covers both the Connect-vs-Kafka-Connect comparison framework and operational essentials (aws-vault, license placement, SIGINT teardown).
---

# Redpanda Connect Bench Framework

This skill helps Redpanda Connect engineers work with the AWS bench framework in `benchmarking/aws/`. It covers three flows: adding a new connector, operating an existing bench, and debugging failures.

> **Body of skill filled in by later tasks. Do not rely on this skeleton as a final document.**
```

- [ ] **Step 3: Verify frontmatter parses**

```bash
head -5 .claude/skills/bench-framework/SKILL.md
```

Expected: `---` line, `name: bench-framework`, `description: Use when...`, `---` close.

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/bench-framework/SKILL.md
git commit -m "$(cat <<'EOF'
feat(skill): bench-framework skill skeleton

Empty SKILL.md with frontmatter; references/ + assets/templates/
directories created. Body content filled in by subsequent tasks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Write `references/traps.md`

**Files:**
- Create: `.claude/skills/bench-framework/references/traps.md`

The trap table is the foundation. Every other file references rows here. Each row is anchored so SKILL.md and `debugging-playbook.md` can deep-link.

Source: `bench-skill-plan` memory (17 traps listed) + `kc-comparison-state` (commit SHAs) + `bench-debugging-history` (symptom detail).

- [ ] **Step 1: Verify referenced commit SHAs still exist**

```bash
for sha in cae256670 3e571c03a a1ba55127 7242bf180 109f315f8 458100ff4 \
           8b47cd202 f1f9e271a 21d96e939 91c45957e 7048a0adf 469593293 \
           655f36783 a22183533 0a318a043 559559c53 79794a84c 992370235 \
           5b0d56306 47ee0d481 5ccb10eb1; do
  git -C /Users/prakhar.garg/Documents/connect_prakhar rev-parse --short "$sha" >/dev/null 2>&1 \
    && echo "$sha OK" \
    || echo "$sha MISSING"
done
```

Expected: All `OK`. If any are missing, find the renamed/squashed equivalent before proceeding.

- [ ] **Step 2: Write `references/traps.md`**

Use this skeleton, one section per trap. Keep each entry ≤ 15 lines.

```markdown
# Bench framework traps

Each entry: what it looks like, why it happens, where the fix lives. Anchors are stable — SKILL.md and other references link in.

<a name="aws-vault"></a>
## 1. Wrap in aws-vault, not AWS_PROFILE

**Symptom:** Mid-bench AWS calls fail with `InvalidGrantException: failed to refresh cached SSO token`.

**Cause:** Plain SSO sessions expire in under an hour; benches take ~90 min. aws-vault mints a 12h STS session that survives the run.

**Where it bit:** Multiple T20 smoke runs (`bench-debugging-history` #13).

**Fix:** Always wrap `task aws:bench` invocations in `aws-vault exec bench --`. No code commit; operational rule.

<a name="license-location"></a>
## 2. License at repo root, NOT in ~/Downloads/

**Symptom:** `stageArtefacts` errors with `operation not permitted` ~8 min into terraform apply.

**Cause:** macOS TCC blocks file reads from Downloads/Documents/Desktop for sandboxed processes.

**Where it bit:** `bench-debugging-history` #9, #14.

**Fix:** Place license at repo root (`/path/to/connect/rpcn.license`). `.gitignore` covers `*.license`. Fail-fast `os.Open` at validate-time landed in `458100ff4`.

<a name="sigint"></a>
## 3. SIGINT (not SIGKILL) for clean teardown

**Symptom:** Orphaned VPC/EC2/IAM/S3 resources after killing a stuck bench.

**Cause:** SIGKILL skips the deferred `terraform destroy`. The runner's signal handler cancels ctx, `MatrixRunner.Run` returns, then `defer destroy` fires.

**Where it bit:** `bench-workflow-essentials` #3.

**Fix:** `pgrep -fl 'benchmarking/aws/runner.*bench'` then `kill <pid>` (default is SIGTERM/SIGINT). NEVER `kill -9`.

<a name="defer-destroy-before-apply"></a>
## 4. defer destroy registered BEFORE first apply

**Symptom:** Failed shared apply orphans VPC/EC2/IAM/S3.

**Cause:** Defer registered after apply; destroy never queued.

**Where it bit:** Early framework versions; fixed in `3e571c03a`.

**Fix:** In `main.go::runBench`, the comment `Register destroy BEFORE any apply` marks the location. Don't refactor without preserving order. `terraform destroy` is idempotent against empty state.

<a name="rds-logical-replication"></a>
## 5. RDS Postgres: rds.logical_replication (not wal_level)

**Symptom:** RDS parameter group apply fails: `Could not find parameter with name: wal_level`.

**Cause:** `wal_level` is not user-settable on RDS Postgres. RDS exposes it as `rds.logical_replication = "1"`.

**Fix:** `terraform/modules/rds-postgres/main.tf` parameter group uses `rds.logical_replication = "1"`. Commit `a1ba55127`. See [rds-quirks.md](rds-quirks.md).

<a name="postgres-cdc-tls"></a>
## 6. postgres_cdc tls field: no `enabled:` toggle

**Symptom:** Config lint error `field enabled not recognised`.

**Cause:** `postgres_cdc` uses `service.NewTLSField` (not `NewTLSToggledField`); the field has no `enabled` sub-key. Setting it explicitly is also required because RDS rejects unencrypted replication connections.

**Fix:** In scenario YAML: `tls: { skip_cert_verify: true }` with no `enabled:` key. Commits `8b47cd202`, `f1f9e271a`.

<a name="workload-rate-per-table"></a>
## 7. workload --rate is per-table-per-sec, not total

**Symptom:** Bench reports throughput far below target.

**Cause:** `scenarios/<x>/<scenario>.yaml` `write_rate_per_sec` is interpreted by the seeder as per-table-per-second. With N tables, total = N × rate.

**Where it bit:** `bench-debugging-history` #17.

**Fix:** Size the workload knowing rate × tables = total. See [scenario-sizing.md](scenario-sizing.md#workload-rate-shape).

<a name="truncate-between-points"></a>
## 8. TRUNCATE between sweep points required

**Symptom:** Later sweep points stall — vCPU 8 returns ~0 MB/s despite vCPU 1/2/4 reporting real numbers.

**Cause:** Table grows unboundedly across sweep points; per-insert latency on the b-tree stretches until the producer stalls.

**Where it bit:** `bench-debugging-history` #18.

**Fix:** Scenario YAML `reset:` block must include `TRUNCATE TABLE <table>`. Initial seed becomes `initial_rows: 0` (the seeder still ensures the table exists).

<a name="redpanda-auto-create-topics"></a>
## 9. Redpanda auto_create_topics_enabled defaults to FALSE

**Symptom:** Both engines silently fail; bench logs `UNKNOWN_TOPIC_OR_PARTITION`.

**Cause:** Redpanda's default is opposite to Apache Kafka. Topics must be created explicitly OR `redpanda.yaml` config must enable auto-create.

**Fix:** Either pre-create topics in reset SQL or enable `redpanda.kafka_api.auto_create_topics_enabled = true` in the Redpanda module. Commit `79794a84c`.

<a name="redpanda-topic-label"></a>
## 10. Redpanda metric label is `redpanda_topic`, not `topic`

**Symptom:** `broker_series` always empty in result JSON despite obvious produce traffic.

**Cause:** Redpanda's `/public_metrics` labels topics as `redpanda_topic`. Code reading `topic` returns nothing.

**Fix:** `extractTopicProduceBytes` in `runner/brokermetrics.go` reads `redpanda_topic`. Commit `559559c53`.

<a name="per-broker-metrics"></a>
## 11. Per-topic byte metrics are per-broker — scrape ALL brokers

**Symptom:** `broker_series` empty for KC even though KC log shows millions of records written.

**Cause:** Redpanda emits `redpanda_kafka_request_bytes_total{redpanda_topic=X}` PER-BROKER, only for partitions that broker leads. Scraping only `broker_ips[0]:9644` misses topics led elsewhere.

**Fix:** TF output `metrics_endpoints` (plural, comma-separated). Bench script iterates all brokers per scrape interval; `extractTopicProduceBytes` sums `+=` across brokers per frame. Commit `5b0d56306`.

<a name="kc-connector-offset-isolation"></a>
## 12. Per-vCPU KC connector name (Debezium offset isolation)

**Symptom:** KC produces 0 MB/s at vCPU ≥ 2 despite vCPU 1 working. KC log: `redo log is no longer available`.

**Cause:** KC stores Debezium offsets in `_kc_offsets` keyed by connector name. Reusing the same name across sweep points means later points try to resume from stale LSN/binlog positions the database has aged out.

**Fix:** Per-vCPU connector name (e.g. `bench_<conn>_v<N>`). Each sweep point gets a fresh offset namespace. Commit `992370235`.

<a name="mysql-debezium-snapshot"></a>
## 13. MySQL Debezium needs snapshot.mode=no_data

**Symptom:** MySQL KC connector fails before warmup; Debezium error references missing offset.

**Cause:** Debezium MySQL with `snapshot.mode=never` requires an existing committed offset. Per-vCPU connector naming creates fresh connectors with no offset. Postgres `pgoutput` is forgiving and can stream from current WAL position; MySQL is stricter.

**Fix:** Use `snapshot.mode=no_data`: snapshot the schema (table is TRUNCATEd → no rows) then stream from current binlog. Commit `47ee0d481`. Encoded in `kcConnectorSpecs["mysql_cdc"]`.

<a name="chrt-fifo-deadlock"></a>
## 14. chrt --fifo deadlocks JVM on single-core taskset

**Symptom:** KC JVM under `taskset -c N-N chrt --fifo 50 ...` stays alive but writes ZERO bytes for 180+ s; never binds port 8083.

**Cause:** SCHED_FIFO is cooperative within a priority class. JVM has many threads (GC, finalizer, JMX, signal dispatcher) — when all bound to one core under FIFO, they deadlock waiting on each other. Go's preemptive runtime is unaffected, so Connect tolerates `chrt --fifo`.

**Fix:** Drop `chrt --fifo 50` from BOTH Connect and KC bench scripts (Plan 3 dropped it from Connect too for scheduler parity). Commit `a22183533`.

<a name="cloud-init-runner-loadgen"></a>
## 15. Cloud-init can't share between runner + load-gen (KC split-brain)

**Symptom:** KC connector PUT returns HTTP 500 / `SocketTimeoutException`.

**Cause:** Plan 1 used one cloud-init template for runner AND load-gen. Both installed kafka-connect and joined the `kc-bench-workers` group. Runner's KC saw load-gen as leader, forwarded PUT to it, runner→load-gen SG blocked 8083.

**Fix:** Parameterize cloud-init with `install_kc`. Runner gets `true`, load-gen gets `false`. Commit `0a318a043`.

<a name="orphan-ttl"></a>
## 16. Orphan-cleanup Lambda 4h TTL trips long benches

**Symptom:** Bench hangs after several hours; infra has been wiped from under it.

**Cause:** EventBridge → Lambda → terraform destroy fires at `BENCH_ORPHAN_TTL_HOURS` (currently 4h). `--keep` flag preserves infra against the runner's `defer destroy` but NOT against the orphan Lambda.

**Fix:** For long benches (8 vCPU × 2 engines), bump `orphan_ttl_hours` in the scenario or in the shared TF stack vars. Commit `5ccb10eb1` bumped 3h→4h.

<a name="bench-session-id-not-output"></a>
## 17. bench_session_id is NOT a TF output — inject in Go

**Symptom:** All Connect topics collapse to `bench__<connector>_connect`; KC cleanup silently skips.

**Cause:** Shared TF stack declares `bench_session_id` as input (for `default_tags`) but doesn't expose it as an output. Downstream code reading `sharedOuts["bench_session_id"]` finds nothing.

**Fix:** Runner injects `bench_session_id` into `sharedOuts` after apply, before passing the map further. Commit `655f36783`.
```

All 17 traps are written above. No additional entries to add in this task.

- [ ] **Step 3: Verify all anchors are unique**

```bash
grep -E '^<a name="' .claude/skills/bench-framework/references/traps.md \
  | sort | uniq -d
```

Expected: empty output (no duplicate anchors).

- [ ] **Step 4: Verify file size**

```bash
wc -l .claude/skills/bench-framework/references/traps.md
```

Expected: ≤ 250 lines. The spec's ≤ 200 line target applies to "small" references — `traps.md` is the load-bearing trap table and is allowed to be the longest reference. If you're significantly over 250, look for entries that have grown too verbose; compress to: Symptom (1-2 lines), Cause (1-2 lines), Where it bit (1 line), Fix (1-2 lines with SHA).

- [ ] **Step 5: Commit**

```bash
git add .claude/skills/bench-framework/references/traps.md
git commit -m "$(cat <<'EOF'
feat(skill): bench-framework traps reference

17 documented traps from Plans 1-3 of the AWS bench effort. Each
entry has a stable anchor for SKILL.md and debugging-playbook.md
to deep-link.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: SKILL.md top-level structure (pre-flight + decision tree)

**Files:**
- Modify: `.claude/skills/bench-framework/SKILL.md`

Replace the skeleton with the actual top-level structure: pre-flight trap summary (linking to traps.md), decision tree, and three H2 placeholders. The placeholders are filled in by Phase B (operate), Phase C (debug), and Phase D (add).

- [ ] **Step 1: Rewrite `.claude/skills/bench-framework/SKILL.md`**

```markdown
---
name: bench-framework
description: Use when working in benchmarking/aws/ on the Redpanda Connect benchmarking framework — adding a new connector bench (TF stack + scenario + engineSpec + kcConnectorSpec), running an existing bench, or debugging a bench failure. Covers both the Connect-vs-Kafka-Connect comparison framework and operational essentials (aws-vault, license placement, SIGINT teardown).
---

# Redpanda Connect Bench Framework

This skill helps engineers work with the AWS bench framework in `benchmarking/aws/`. The framework runs a Connect-vs-Kafka-Connect head-to-head sweep across vCPU points on the same hardware, with broker-side throughput as the canonical metric.

**Assumes:** Connect experience, bench-new. The skill explains bench concepts (sweep points, vCPU pinning, fairness) but does not re-teach Redpanda Connect itself.

## Pre-flight: traps to know before you start

Each line links to [references/traps.md](references/traps.md). Skim before any bench session — these are non-obvious and have all bitten in production.

| Trap | One-liner |
|------|-----------|
| 1 | Wrap `task aws:bench` in `aws-vault exec bench`, not `AWS_PROFILE=`. [link](references/traps.md#aws-vault) |
| 2 | Place the Connect Enterprise license at the repo root, not `~/Downloads/`. [link](references/traps.md#license-location) |
| 3 | Stop a stuck bench with SIGINT (Ctrl+C), never SIGKILL. [link](references/traps.md#sigint) |
| 4 | `defer destroy` is registered BEFORE the first `terraform apply`. Don't refactor that order. [link](references/traps.md#defer-destroy-before-apply) |
| 5 | RDS Postgres uses `rds.logical_replication`, not `wal_level`. [link](references/traps.md#rds-logical-replication) |
| 6 | `postgres_cdc` tls block has no `enabled:` toggle. [link](references/traps.md#postgres-cdc-tls) |
| 7 | `write_rate_per_sec` is per-table-per-second, not total. [link](references/traps.md#workload-rate-per-table) |
| 8 | TRUNCATE between sweep points required. [link](references/traps.md#truncate-between-points) |
| 9 | Redpanda's `auto_create_topics_enabled` defaults FALSE (opposite of Apache Kafka). [link](references/traps.md#redpanda-auto-create-topics) |
| 10 | Metric label is `redpanda_topic`, not `topic`. [link](references/traps.md#redpanda-topic-label) |
| 11 | Per-topic byte metrics are per-broker — scrape ALL brokers. [link](references/traps.md#per-broker-metrics) |
| 12 | Per-vCPU KC connector name for offset isolation: `bench_<conn>_v<N>`. [link](references/traps.md#kc-connector-offset-isolation) |
| 13 | MySQL Debezium needs `snapshot.mode=no_data`, not `never`. [link](references/traps.md#mysql-debezium-snapshot) |
| 14 | Don't use `chrt --fifo` with JVM — deadlocks under single-core taskset. [link](references/traps.md#chrt-fifo-deadlock) |
| 15 | Runner and load-gen need DIFFERENT cloud-init templates (KC split-brain). [link](references/traps.md#cloud-init-runner-loadgen) |
| 16 | Orphan-cleanup Lambda's 4h TTL can trip long benches. [link](references/traps.md#orphan-ttl) |
| 17 | `bench_session_id` is NOT a TF output — runner injects it post-apply. [link](references/traps.md#bench-session-id-not-output) |

## What are you doing?

1. **Adding a new connector bench** → [Add a new connector](#add-a-new-connector). Skill walks 9 steps to produce the TF stack, scenario YAML, engineSpec entry, kcConnectorSpec entry, reset SQL, plus tests.
2. **Running an existing bench** → [Operate a bench](#operate-a-bench). Skill walks the pre-flight checklist, the run, live monitoring, and teardown.
3. **Debugging a failed bench** → [Debug](#debug). Skill triages from symptom into the playbook.

## Add a new connector

> _Filled in by Phase D._

## Operate a bench

> _Filled in by Phase B._

## Debug

> _Filled in by Phase C._
```

- [ ] **Step 2: Verify all trap-table links resolve**

```bash
# Extract anchors from traps.md
grep -E '^<a name="' .claude/skills/bench-framework/references/traps.md \
  | sed 's/.*name="//;s/".*//' | sort > /tmp/anchors-defined.txt

# Extract anchor references from SKILL.md
grep -oE 'traps\.md#[a-z-]+' .claude/skills/bench-framework/SKILL.md \
  | sed 's/traps.md#//' | sort | uniq > /tmp/anchors-used.txt

# Any used-but-not-defined?
comm -23 /tmp/anchors-used.txt /tmp/anchors-defined.txt
```

Expected: empty output. If anything appears, the SKILL.md table references an anchor that doesn't exist in traps.md — fix one or the other.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/bench-framework/SKILL.md
git commit -m "$(cat <<'EOF'
feat(skill): SKILL.md top-level structure + trap summary

Pre-flight trap table linking into references/traps.md, plus the
3-way decision tree. Branch sections are placeholders filled in
by later phases.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase B: Operate branch

### Task 4: Write `references/workflow-essentials.md`

**Files:**
- Create: `.claude/skills/bench-framework/references/workflow-essentials.md`

Source: `bench-workflow-essentials` memory. Six rules, one per H2.

- [ ] **Step 1: Write the file**

```markdown
# Bench workflow essentials

Non-negotiables when running an AWS bench. Each was discovered by failure during real runs.

## 1. Always wrap in `aws-vault exec`, never `AWS_PROFILE=`

Plain SSO sessions expire in under an hour. A bench takes ~90 min. The Go runner makes AWS SDK calls (S3 upload, SSM RunCommand) for the full duration; mid-run SSO refresh fails with `InvalidGrantException`. aws-vault mints a 12-hour STS session that the SDK uses throughout.

```bash
aws-vault exec bench -- task aws:bench scenario=<path>
```

The profile name `bench` corresponds to the account documented in the `aws-bench-account` memory (account 211125438402, us-east-2).

See also: [traps.md#aws-vault](traps.md#aws-vault).

## 2. License file lives at the repo root

The Connect Enterprise license file must live at `<repo-root>/rpcn.license`. macOS TCC blocks file reads from `~/Downloads/`, `~/Documents/`, `~/Desktop/` for sandboxed processes. The Go runner (launched via `task aws:bench`) inherits the sandbox and `os.Open` fails with "operation not permitted" if the license is in those directories.

`.gitignore` already covers `*.license` — placing it at repo root does not risk committing.

See also: [traps.md#license-location](traps.md#license-location).

## 3. SIGINT, not SIGKILL

To kill a stuck bench: find the runner binary's pid and send default-signal `kill`. SIGKILL skips the deferred `terraform destroy` and leaves orphan VPC/EC2/IAM/S3.

```bash
pgrep -fl 'benchmarking/aws/runner.*bench'
# Pick the one without 'go run' in the line — that's the compiled binary
kill <pid>
```

`TaskStop` from the agent harness also kills without giving the defer time to run. Avoid for the bench process.

See also: [traps.md#sigint](traps.md#sigint).

## 4. `defer destroy` is registered BEFORE the first `terraform apply`

If you're modifying `runner/main.go::runBench`, search for the comment `Register destroy BEFORE any apply` — don't refactor without preserving the order. `terraform destroy` is idempotent against empty state, so registering it pre-apply is safe.

See also: [traps.md#defer-destroy-before-apply](traps.md#defer-destroy-before-apply).

## 5. `task aws:validate` is free

It runs locally (no AWS calls). Use it whenever a scenario YAML changes, before any `aws:bench`. Catches typos and shape errors that would otherwise burn a real bench.

```bash
task aws:validate scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml
```

## 6. Use `--keep-on-fail` for live debug

When a bench fails and you want to SSH into the runner to investigate before infra is destroyed, pass `--keep-on-fail` to `task aws:bench`. Tears down on success; preserves on failure. Pair with explicit `task aws:down` when you're done.

When investigating mid-run failures with `--keep-on-fail`, remember the orphan-cleanup Lambda (4h TTL) — see [traps.md#orphan-ttl](traps.md#orphan-ttl).
```

- [ ] **Step 2: Verify links to traps.md resolve**

```bash
grep -oE 'traps\.md#[a-z-]+' .claude/skills/bench-framework/references/workflow-essentials.md \
  | sed 's/traps.md#//' | sort | uniq > /tmp/anchors-used.txt
comm -23 /tmp/anchors-used.txt /tmp/anchors-defined.txt
```

Expected: empty output.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/bench-framework/references/workflow-essentials.md
git commit -m "$(cat <<'EOF'
feat(skill): workflow-essentials reference

Six non-negotiables: aws-vault wrap, license location, SIGINT for
teardown, defer ordering, validate before apply, --keep-on-fail
for live debug.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Fill in "Operate a bench" section in SKILL.md

**Files:**
- Modify: `.claude/skills/bench-framework/SKILL.md`

Replace the `## Operate a bench` placeholder with the 8-step walkthrough from the spec.

- [ ] **Step 1: Replace the placeholder section**

Find the line `## Operate a bench` followed by the `> _Filled in by Phase B._` placeholder. Replace with:

```markdown
## Operate a bench

If you're running an existing bench (not adding a new connector), walk this checklist top-to-bottom. The pre-flight is mandatory; the rest is the actual run.

### Step 1: Pre-flight checklist

- [ ] `aws-vault` installed; profile `bench` configured (account 211125438402, us-east-2)
- [ ] Redpanda Connect Enterprise license at repo root as `rpcn.license` (NOT `~/Downloads/`)
- [ ] On `benchmarking` branch with latest commits (`git pull origin benchmarking`)
- [ ] `make zip` run inside `benchmarking/aws/cleanup-lambda/` recently (orphan cleanup needs the artifact)

Full rationale: [references/workflow-essentials.md](references/workflow-essentials.md).

### Step 2: Pick the scenario

List existing scenarios:

```bash
ls benchmarking/aws/scenarios/*/*.yaml
```

Wall-clock and spend estimates:

| Scope | Wall-clock | Spend |
|-------|------------|-------|
| 1-vCPU smoke | ~25 min | ~$1.50 |
| 4-point sweep × 2 engines | ~2.5-3h | ~$8 |
| Full 4-point sweep × 2 engines at 150K writes/sec | ~3-4h | ~$10-12 |

Long benches (>4h) risk the orphan-cleanup TTL — see [traps.md#orphan-ttl](references/traps.md#orphan-ttl).

### Step 3: Validate (free, no AWS spend)

```bash
task aws:validate scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml
```

Catches YAML typos, missing engineSpec/kcConnectorSpec entries, sizing-trap candidates.

### Step 4: Run

```bash
aws-vault exec bench -- task aws:bench \
  scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml \
  [--engines connect,kafka_connect] \
  [--keep-on-fail]
```

- `--engines` defaults to `connect` only. Pass both to get the head-to-head sweep.
- KC roughly doubles wall-clock per vCPU point (sequential, same runner host).
- `--keep-on-fail` preserves infra for live debug — see [references/workflow-essentials.md#6](references/workflow-essentials.md).

### Step 5: Live monitoring

While the bench runs:

- Heartbeat lines every 60s show rolling stats. "Good" = steady ≥ 20 MB/s once warmup ends.
- S3 paths to check (replace `<sess>` with the session id printed at startup):
  - `s3://<bucket>/runs/<sess>/sweep-<vcpu>.log` — Connect's full output per point
  - `s3://<bucket>/runs/<sess>/prom-<vcpu>.txt` — `:4195/metrics` snapshots
  - `s3://<bucket>/runs/<sess>/redpanda-{connect,kc}-<vcpu>.txt` — broker `/public_metrics`
  - `s3://<bucket>/runs/<sess>/kc-<vcpu>.log` — KC JVM output

If you need to interrupt: **SIGINT (Ctrl+C) only**. SIGKILL strands infra. See [traps.md#sigint](references/traps.md#sigint).

### Step 6: Teardown

Happy path: bench finishes, `defer destroy` fires automatically.

Hung-bench path:

```bash
aws-vault exec bench -- task aws:down
```

If `task aws:down` fails with `bench_session_id: required variable not set`, see [debugging-playbook.md#tf-destroy-session-id](references/debugging-playbook.md#tf-destroy-session-id).

### Step 7: Inspect results

```bash
ls benchmarking/aws/results/<connector>/<scenario>/
```

Each run produces a JSON + adjacent markdown. The auto-refreshed `benchmarking/aws/SUMMARY.md` table aggregates the latest run per scenario.

How to read the per-run markdown:

| Column | Meaning |
|--------|---------|
| `engine` | `connect` or `kafka_connect` |
| `MB/sec (p50)` | Median throughput from rolling stats |
| `broker MB/s` | Broker-derived median (canonical fairness metric) |
| `MB/sec (p5)` / `(p95)` | Tail spread |
| `msg/sec (p50)` | Median msg rate |
| `Δ vs Connect` | Diff vs Connect at same vCPU (only on the KC row) |
| `⚠` flag on SUMMARY row | Cross-engine divergence > 2× detected |

### Step 8: On failure

Go to [Debug](#debug).
```

- [ ] **Step 2: Verify SKILL.md line count is ≤ 300**

```bash
wc -l .claude/skills/bench-framework/SKILL.md
```

Expected: ≤ 300 lines.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/bench-framework/SKILL.md
git commit -m "$(cat <<'EOF'
feat(skill): operate-a-bench section in SKILL.md

8-step checklist: pre-flight, pick scenario, validate, run with
aws-vault wrap, live monitoring (S3 paths + SIGINT-only), teardown,
inspect results.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase C: Debug branch

### Task 6: Write `references/debugging-playbook.md`

**Files:**
- Create: `.claude/skills/bench-framework/references/debugging-playbook.md`

Source: `bench-debugging-history` memory + `kc-comparison-state` Plan 2/3 narratives. Each entry: symptom, root cause, fix commit SHA, what to do now.

- [ ] **Step 1: Write the file**

```markdown
# Debugging playbook

Symptom-keyed lookup. Each section is anchored so SKILL.md and other references can deep-link. The pattern: symptom → root cause → existing fix (commit SHA) → what you should do.

<a name="empty-workload"></a>
## Connect reports `Summary.MedianMBPerSec=0` with no `[load]` lines

**Symptom:** All 899+ samples report `msg/sec=0 bytes/sec=0`. Plan 1's smoke would produce 25 MB/s at the same shape.

**Likely cause:** The workload subcommand of the seeder (`benchmarking/aws/seeders/<seeder>/`) is either silent on success or never wrote rows. With `output: redpanda` instead of `output: drop`, runtime issues that drop's no-op masked are now visible.

**Where it bit:** Plan 2 smoke first run.

**Investigate:**

1. Look for `[load]` lines in the sweep log on S3. None? Workload generator silent.
2. Compare against Plan 1 smoke at same shape with `output: drop`.
3. SSH/SSM into runner, manually invoke the workload subcommand: `POSTGRES_DSN=<dsn> /opt/bench/seeders/cdc-rows workload --rate=20000 --duration=30s`. Look for stderr.

<a name="kc-http-500"></a>
## KC connector PUT returns HTTP 500 with no body

**Symptom:** Bench logs `curl -fsS -X PUT ... → HTTP 500`. No body printed.

**Likely cause:** `curl --fail (-f)` suppressed the response body.

**Fix:** Already addressed in `f59b98cf1` — KC bench script uses `curl -sS -o /tmp/resp -w '%{http_code}' ...` and cats the body on non-2xx. If you're seeing this now, the KC script in `runner/kcscript.go` may have been edited.

**Verify:** `git log -p benchmarking/aws/runner/kcscript.go | grep -A2 'HTTP_CODE'`

<a name="broker-series-empty"></a>
## `broker_series` empty even though KC log shows writes

**Symptom:** Result JSON's `BrokerSeries` is null/empty for KC despite `kc-<vcpu>.log` showing millions of records produced.

**Likely cause:** Either (a) the parser is reading the wrong label name (`topic` instead of `redpanda_topic`) — see [traps.md#redpanda-topic-label](traps.md#redpanda-topic-label); or (b) the per-broker metric exposition isn't being aggregated — see [traps.md#per-broker-metrics](traps.md#per-broker-metrics).

**Diagnosis:**

```bash
# Pull a sample frame from the live broker:
aws ssm start-session --target <broker-instance-id>
# inside the session:
curl -s http://localhost:9644/public_metrics | grep redpanda_kafka_request_bytes_total | head
```

If labels are `redpanda_topic=` and you see your topic, but `broker_series` is still empty, you're scraping only one broker (case b).

**Fixes:**
- Label name: `559559c53`
- Multi-broker scrape: `5b0d56306` — new TF output `metrics_endpoints`, bench script iterates all 3 brokers per scrape interval.

<a name="auto-create"></a>
## `UNKNOWN_TOPIC_OR_PARTITION` for every write

**Symptom:** Both engines silently fail; bench logs show repeated `UNKNOWN_TOPIC_OR_PARTITION`.

**Root cause:** Redpanda's default `auto_create_topics_enabled=false` (opposite of Apache Kafka).

**Fix:** `79794a84c` — enable `kafka_api.auto_create_topics_enabled=true` in the Redpanda module's `redpanda.yaml`. Or pre-create topics in the reset block.

<a name="offset-isolation"></a>
## KC at vCPU≥2 reports 0 MB/s; KC log says `redo log is no longer available`

**Symptom:** KC works at vCPU=1, then 0 MB/s for vCPU=2 and beyond.

**Root cause:** Debezium offsets persist in `_kc_offsets` across sweep points. Reusing the same connector name means sweep point N+1 tries to resume from the LSN/binlog position of sweep point N. The DB has aged that position out.

**Fix:** Per-vCPU connector name (`bench_<conn>_v<N>`). Each sweep point gets a fresh offset namespace. Commit `992370235`.

**Verify:** In `runner/kcscript.go`, look for the connector name being rendered with the vCPU count.

<a name="dns-flake"></a>
## RDS DNS timeout mid-bench

**Symptom:** A reset step (or any RDS call) fails with `could not translate host name "<rds-endpoint>" to address: Name or service not known`. First reset of the same bench worked.

**Root cause:** VPC DNS resolver flake. Same runner instance, same RDS endpoint — DNS state degrades across long sessions.

**Mitigation:** Retry the reset with `getent hosts` precondition, or add a `nslookup` to the reset script preface for diagnostics. Currently not patched in code; the bench will fail and need a fresh run.

<a name="orphan-ttl"></a>
## Bench hangs after several hours; SSM calls return "InstanceId not found"

**Symptom:** Bench wall-clock crosses 4h boundary, then SSM commands fail with the target instance unknown.

**Root cause:** EventBridge → orphan-cleanup Lambda fired at the TTL window. EC2 + S3 already destroyed. Bench process keeps trying to talk to ghosts.

**Mitigation:** Bump `orphan_ttl_hours` in scenario or shared TF stack for long benches. `5ccb10eb1` raised default from 3h → 4h.

<a name="tf-destroy-session-id"></a>
## `task aws:down` fails: `bench_session_id: required variable not set`

**Symptom:** Manual teardown after a hung bench fails.

**Root cause:** `runner/main.go::downCmd` passes only `region` and `runner_instance_type`. Shared stack declares `bench_session_id` as required (no default). Destroy doesn't need the tag value but TF can't pass var validation.

**Workaround:**

```bash
cd benchmarking/aws/terraform/shared && \
  aws-vault exec bench -- terraform init -reconfigure \
    -backend-config=../backend.hcl \
    -backend-config=key=shared/terraform.tfstate && \
  terraform destroy -auto-approve \
    -var=bench_session_id=teardown-$(date +%Y%m%d) \
    -var=runner_instance_type=c8g.xlarge
```

**Follow-up:** Give `bench_session_id` a `""` default OR have `downCmd` read it from state. Documented in `bench-debugging-history` #30.

<a name="terraform-init-backend-changed"></a>
## `terraform init` fails: "Backend configuration changed"

**Symptom:** Bench fails immediately at first apply with `terraform init shared: exit status 1`.

**Root cause:** Earlier `terraform init -backend=false` during development left cached `.terraform/` directories with `backend=local` config. The runner's real-S3 init detects the mismatch.

**Fix:**

```bash
find benchmarking/aws/terraform -type d -name '.terraform' -exec rm -rf {} +
```

Then retry. They're gitignored, so it's safe.

**Pattern:** If you ran `terraform init -backend=false` for validation during dev, blow away `.terraform/` before the next live bench.

<a name="rds-dynamodb-lock"></a>
## `ConditionalCheckFailedException: Error acquiring the state lock`

**Symptom:** Bench fails at `terraform apply shared` immediately after a prior bench's destroy.

**Root cause:** Stale DynamoDB lock entry from the prior run's destroy phase, OR DDB lock table missing entirely.

**Fix (stale lock):**

```bash
aws-vault exec bench -- terraform force-unlock -force <lock-id>
```

**Fix (table missing — bench-debugging-history #27):**

```bash
aws-vault exec bench -- aws dynamodb create-table \
  --table-name redpanda-connect-bench-tflocks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-2
```

**Rule:** Wait ~30s between back-to-back benches.
```

- [ ] **Step 2: Verify cross-references**

```bash
# debugging-playbook.md links into traps.md — check anchors resolve
grep -oE 'traps\.md#[a-z-]+' .claude/skills/bench-framework/references/debugging-playbook.md \
  | sed 's/traps.md#//' | sort | uniq > /tmp/anchors-used.txt
comm -23 /tmp/anchors-used.txt /tmp/anchors-defined.txt
```

Expected: empty.

- [ ] **Step 3: Verify all commit SHAs cited still exist**

```bash
for sha in f59b98cf1 559559c53 5b0d56306 79794a84c 992370235 5ccb10eb1; do
  git -C /Users/prakhar.garg/Documents/connect_prakhar rev-parse --short "$sha" >/dev/null 2>&1 \
    && echo "$sha OK" || echo "$sha MISSING"
done
```

Expected: All `OK`.

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/bench-framework/references/debugging-playbook.md
git commit -m "$(cat <<'EOF'
feat(skill): debugging-playbook reference

Symptom-keyed lookup covering the 10 most common failure modes
from Plans 1-3 (workload silent, KC HTTP 500, broker_series empty,
auto-create, offset isolation, DNS flake, orphan TTL, TF state-lock,
etc.). Each entry points at the existing fix SHA.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Fill in "Debug" section in SKILL.md

**Files:**
- Modify: `.claude/skills/bench-framework/SKILL.md`

- [ ] **Step 1: Replace the `## Debug` placeholder**

Find the `## Debug` heading and its placeholder `> _Filled in by Phase C._`. Replace with:

```markdown
## Debug

Look up the symptom in the table below, then read the linked playbook entry. Each entry ends with the existing fix's commit SHA.

| Symptom | Likely cause | Playbook |
|---------|--------------|----------|
| Connect `MedianMBPerSec=0`, no `[load]` lines | Workload generator silent | [link](references/debugging-playbook.md#empty-workload) |
| KC HTTP 500 with no body | `curl --fail` suppressed body | [link](references/debugging-playbook.md#kc-http-500) |
| `broker_series` empty though KC log shows writes | Wrong label name OR single-broker scrape | [link](references/debugging-playbook.md#broker-series-empty) |
| `UNKNOWN_TOPIC_OR_PARTITION` errors | Redpanda `auto_create_topics_enabled=false` | [link](references/debugging-playbook.md#auto-create) |
| KC at vCPU≥2 reports 0 MB/s, log says `redo log is no longer available` | Debezium offset persistence | [link](references/debugging-playbook.md#offset-isolation) |
| `could not translate host name` RDS endpoint | VPC DNS flake | [link](references/debugging-playbook.md#dns-flake) |
| Bench hangs after several hours, SSM "InstanceId not found" | Orphan-cleanup Lambda fired | [link](references/debugging-playbook.md#orphan-ttl) |
| `task aws:down` fails: `bench_session_id required` | Shared stack declares it required | [link](references/debugging-playbook.md#tf-destroy-session-id) |
| `terraform init shared: exit status 1` "Backend configuration changed" | Stale `.terraform/` dirs | [link](references/debugging-playbook.md#terraform-init-backend-changed) |
| `ConditionalCheckFailedException: Error acquiring the state lock` | Stale DDB lock or missing table | [link](references/debugging-playbook.md#rds-dynamodb-lock) |

If the symptom isn't here, search [traps.md](references/traps.md) by keyword and check git log for recent fixes on the `benchmarking` branch.
```

- [ ] **Step 2: Verify all playbook anchors resolve**

```bash
grep -E '^<a name="' .claude/skills/bench-framework/references/debugging-playbook.md \
  | sed 's/.*name="//;s/".*//' | sort > /tmp/playbook-defined.txt
grep -oE 'debugging-playbook\.md#[a-z-]+' .claude/skills/bench-framework/SKILL.md \
  | sed 's/debugging-playbook.md#//' | sort | uniq > /tmp/playbook-used.txt
comm -23 /tmp/playbook-used.txt /tmp/playbook-defined.txt
```

Expected: empty.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/bench-framework/SKILL.md
git commit -m "$(cat <<'EOF'
feat(skill): debug section in SKILL.md

10-symptom triage table linking into debugging-playbook.md. Each
row keyed by user-visible symptom; each link lands on the
specific playbook entry with the existing fix SHA.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase D: Add-connector branch

### Task 8: Write `references/rds-quirks.md`

**Files:**
- Create: `.claude/skills/bench-framework/references/rds-quirks.md`

Source: `bench-rds-quirks` memory.

- [ ] **Step 1: Write the file**

```markdown
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
```

- [ ] **Step 2: Verify anchor links resolve**

```bash
grep -oE 'traps\.md#[a-z-]+' .claude/skills/bench-framework/references/rds-quirks.md \
  | sed 's/traps.md#//' | sort | uniq > /tmp/anchors-used.txt
comm -23 /tmp/anchors-used.txt /tmp/anchors-defined.txt
```

Expected: empty.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/bench-framework/references/rds-quirks.md
git commit -m "$(cat <<'EOF'
feat(skill): rds-quirks reference

Postgres parameter group rules (rds.logical_replication, not
wal_level), postgres_cdc TLS field shape (NewTLSField has no
enabled:), instance class minimums, plus the wrong backup-window
hypothesis warning.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Write `references/kc-connector-mapping.md`

**Files:**
- Create: `.claude/skills/bench-framework/references/kc-connector-mapping.md`

- [ ] **Step 1: Write the file**

```markdown
# Kafka Connect connector mapping

For every Redpanda Connect connector that has a comparison bench, pick the right Kafka Connect plugin. The mapping lives in `benchmarking/aws/runner/kcconnectors.go::kcConnectorSpecs`.

## Installed plugins

The runner's cloud-init installs these:

| Plugin | Class | Direction | Source |
|--------|-------|-----------|--------|
| Debezium PostgreSQL 2.7.x | `io.debezium.connector.postgresql.PostgresConnector` | Source (CDC) | Debezium Apache 2.0 |
| Debezium MySQL 2.7.x | `io.debezium.connector.mysql.MySqlConnector` | Source (CDC) | Debezium Apache 2.0 |
| Aiven JDBC Sink 6.10.0 | `io.aiven.connect.jdbc.JdbcSinkConnector` | Sink | Aiven Apache 2.0 |
| Aiven JDBC Source 6.10.0 | `io.aiven.connect.jdbc.JdbcSourceConnector` | Source (polling) | Aiven Apache 2.0 |
| Confluent S3 Sink (deferred to Plan 4) | TBD | Sink | — |

## Picking the plugin for a new connector

| Redpanda Connect connector | KC plugin | Why |
|----------------------------|-----------|-----|
| `postgres_cdc` | Debezium PostgreSQL | Both speak logical replication / pgoutput |
| `mysql_cdc` | Debezium MySQL | Both speak binlog |
| `sqlserver_cdc` | Debezium SQL Server (NOT INSTALLED — add to cloud-init) | Both speak CDC table tail |
| `mongo_cdc` | Debezium MongoDB (NOT INSTALLED) | Both speak change-stream / oplog |
| Postgres polling-source | Aiven JDBC Source | Both poll a query |
| S3 sink | Confluent S3 Sink (Plan 4 placeholder) | Both write objects to S3 |
| Iceberg sink | Tabular Iceberg sink (Plan 4 placeholder) | Both write Iceberg tables |
| Kafka → Kafka MM | MirrorMaker 2 (bundled but not exercised) | Both replicate topics |

**For CDC sources without a Debezium equivalent**, polling via Aiven JDBC Source is a fallback but is NOT a fair comparison — Connect's CDC reads the replication stream directly while JDBC Source uses timestamp/incrementing-key polling.

## Adding a new KC plugin (cloud-init step)

If the connector you're benching needs a plugin not in the list above:

1. Find the Confluent Hub / GitHub release URL for the connector's `*.zip` distribution.
2. In `benchmarking/aws/terraform/shared/runner-user-data.tftpl`, add a step in the cloud-init runcmd to download + unzip into `/opt/kafka/plugins/<name>/`.
3. Restart the kafka-connect systemd unit (`systemctl restart kafka-connect`) so KC's plugin scan picks it up.
4. Verify on the runner: `curl -s localhost:8083/connector-plugins | jq -r '.[].class'` must include the new connector's class.
5. Add the class + props template to `kcConnectorSpecs` in `runner/kcconnectors.go`.

## kcConnectorSpec template gotchas

When you add an entry to `kcConnectorSpecs`:

1. **`Direction`** must be `kcSource` or `kcSink`. Affects how the reset hooks treat topic teardown.
2. **`PropsTemplate`** is `text/template`-rendered. Inputs are documented next to `renderKCConfig` (the renderer in `runner/kcconnectors.go`). For Debezium SQL Server, you'll likely need `Host/Port/User/Password/Database/SchemaTables/TopicPrefix` (same shape as Postgres).
3. **`snapshot.mode`** — copy MySQL's reasoning: if your engine requires a snapshot to bootstrap an offset, use `no_data` so the schema is captured without the rows. If your engine can stream from current position without an offset (like pgoutput), use `never`.
4. **`RequiredPlugins`** is a `glob` list checked against `curl :8083/connector-plugins`. Must match the installed plugin's class glob (e.g. `debezium-connector-sqlserver*`).
```

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/bench-framework/references/kc-connector-mapping.md
git commit -m "$(cat <<'EOF'
feat(skill): kc-connector-mapping reference

Installed-plugin inventory, plugin-picking decision table for new
connectors, cloud-init steps for adding a new KC plugin, and
kcConnectorSpec template gotchas (Direction, PropsTemplate,
snapshot.mode, RequiredPlugins glob).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Write `references/scenario-sizing.md`

**Files:**
- Create: `.claude/skills/bench-framework/references/scenario-sizing.md`

Source: `bench-scenario-sizing-lessons` memory + observed ceilings.

- [ ] **Step 1: Write the file**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/bench-framework/references/scenario-sizing.md
git commit -m "$(cat <<'EOF'
feat(skill): scenario-sizing reference

Three sizing traps (workload-rate shape, TRUNCATE between points,
observed ceilings on db.r6g.2xlarge), recipe blocks for postgres
and mysql shapes, and a checklist for new scenarios.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Write `references/exemplar-tour.md`

**Files:**
- Create: `.claude/skills/bench-framework/references/exemplar-tour.md`

Source: `bench-framework-code-map` memory (trimmed). Provides the file-by-file walk for postgres + mysql_cdc artifacts an engineer should read before adding a new one.

- [ ] **Step 1: Write the file**

```markdown
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
```

- [ ] **Step 2: Verify file path references**

```bash
# Make sure cited files exist
for f in \
  benchmarking/aws/terraform/stacks/postgres/main.tf \
  benchmarking/aws/terraform/stacks/postgres/variables.tf \
  benchmarking/aws/terraform/stacks/postgres/outputs.tf \
  benchmarking/aws/scenarios/postgres/orders-cdc.yaml \
  benchmarking/aws/scenarios/mysql/orders-cdc.yaml \
  benchmarking/aws/runner/scenario.go \
  benchmarking/aws/runner/kcconnectors.go \
  benchmarking/aws/runner/scripts.go; do
  test -f "$f" && echo "$f OK" || echo "$f MISSING"
done
```

Expected: All `OK`. If any are missing, the file has been renamed since the memory was captured — update the reference.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/bench-framework/references/exemplar-tour.md
git commit -m "$(cat <<'EOF'
feat(skill): exemplar-tour reference

File-by-file tour of postgres_cdc and mysql_cdc artifacts: TF
stacks, scenarios, engineSpec entries, kcConnectorSpec entries,
reset blocks. The mental model an engineer needs before mirroring
into a new connector.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: Create `assets/templates/` scaffolding

**Files:**
- Create: `.claude/skills/bench-framework/assets/templates/tf-stack/main.tf.tmpl`
- Create: `.claude/skills/bench-framework/assets/templates/tf-stack/variables.tf.tmpl`
- Create: `.claude/skills/bench-framework/assets/templates/tf-stack/outputs.tf.tmpl`
- Create: `.claude/skills/bench-framework/assets/templates/scenario.yaml.tmpl`
- Create: `.claude/skills/bench-framework/assets/templates/engine-spec.go.tmpl`
- Create: `.claude/skills/bench-framework/assets/templates/kc-connector-spec.go.tmpl`
- Create: `.claude/skills/bench-framework/assets/templates/reset.sql.tmpl`

All templates use the same placeholder syntax: `{{CONNECTOR}}` (e.g. `sqlserver_cdc`), `{{ENGINE}}` (e.g. `sqlserver`), `{{TABLE}}` (e.g. `orders`).

- [ ] **Step 1: Write `tf-stack/main.tf.tmpl`**

```hcl
# Terraform stack for {{CONNECTOR}} bench.
#
# Composes the shared infra (VPC, IAM, S3, runner+load-gen EC2, Redpanda
# cluster) with the {{ENGINE}}-specific RDS module. Mirror the postgres
# and mysql stacks at terraform/stacks/{postgres,mysql}/.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project           = "redpanda-connect-bench"
      "bench-session-id" = var.bench_session_id
      ManagedBy         = "terraform"
    }
  }
}

data "terraform_remote_state" "shared" {
  backend = "s3"
  config = {
    bucket = "redpanda-connect-bench-tfstate"
    key    = "shared/terraform.tfstate"
    region = var.region
  }
}

module "{{ENGINE}}" {
  source = "../../modules/rds-{{ENGINE}}"  # TODO: create this module if it doesn't exist

  vpc_id              = data.terraform_remote_state.shared.outputs.vpc_id
  subnet_ids          = data.terraform_remote_state.shared.outputs.subnet_ids
  runner_sg_id        = data.terraform_remote_state.shared.outputs.runner_sg_id
  bench_session_id    = var.bench_session_id

  instance_class      = var.source_instance_class
  storage_gb          = var.source_storage_gb
  iops                = var.source_iops
  parameters          = var.source_parameters
}
```

- [ ] **Step 2: Write `tf-stack/variables.tf.tmpl`**

```hcl
variable "region" {
  type    = string
}

variable "runner_instance_type" {
  type    = string
}

variable "bench_session_id" {
  type    = string
}

variable "source_instance_class" {
  type    = string
  default = "db.r6g.2xlarge"
}

variable "source_storage_gb" {
  type    = number
  default = 400
}

variable "source_iops" {
  type    = number
  default = 12000
}

variable "source_parameters" {
  type    = map(string)
  default = {}
}
```

- [ ] **Step 3: Write `tf-stack/outputs.tf.tmpl`**

```hcl
# Outputs consumed by engineSpec[<connector>] in runner/scenario.go.
#
# DSNOutputKey: full connection-string URL the runner exports as DSNEnvVar
# Reset*OutputKey: discrete fields used when the engine CLI does not accept a DSN URL
#   (e.g. mysql client wants -h/-P/-u/-p, not a URL). For DSN-accepting CLIs (psql),
#   leave the Reset*OutputKey fields empty in engineSpec and the discrete outputs unused.

output "{{ENGINE}}_dsn" {
  value     = module.{{ENGINE}}.dsn
  sensitive = true
}

# Discrete fields — populate ONLY if the engine CLI doesn't accept a DSN URL.
# Delete this block if your engineSpec leaves Reset*OutputKey empty.
output "{{ENGINE}}_host"     { value = module.{{ENGINE}}.host }
output "{{ENGINE}}_port"     { value = module.{{ENGINE}}.port }
output "{{ENGINE}}_user"     { value = module.{{ENGINE}}.user }
output "{{ENGINE}}_password" { value = module.{{ENGINE}}.password sensitive = true }
output "{{ENGINE}}_db"       { value = module.{{ENGINE}}.db }
```

- [ ] **Step 4: Write `scenario.yaml.tmpl`**

```yaml
name: {{ENGINE}}-{{TABLE}}-cdc
description: |
  Stream changes from a {{ENGINE}} {{TABLE}} table under sustained heavy writes
  so the {{CONNECTOR}} input — not the producer — is the bottleneck across the
  CPU sweep. TRUNCATE between sweep points keeps the table size bounded.

# NOTE: every comment below is from a trap that bit during postgres/mysql benches.
# Read references/scenario-sizing.md and references/traps.md before changing.

connector: {{CONNECTOR}}
stack: {{ENGINE}}

infra:
  source:
    instance_class: db.r6g.2xlarge          # ≥ this for sustained 50K+ msg/sec; see rds-quirks.md
    storage_gb: 400                          # 400+ required when iops is set
    iops: 12000
    parameters:
      # Engine-specific. For postgres: rds.logical_replication, max_wal_senders.
      # For mysql: binlog_format. Add your engine's required parameters.
      # See references/rds-quirks.md.
  runner:
    instance_type: c8g.4xlarge               # Standard runner; matches postgres/mysql.

dataset:
  initial_rows: 0                            # TRUNCATE-between-points makes a seed pointless
  row_size_bytes: 1200
  tables: [{{TABLE}}]
  seeder: cdc-rows                           # Default seeder; replace if you wrote a custom one

workload:
  write_rate_per_sec: 150000                 # PER-TABLE-PER-SEC; with N tables, total = N × this
  duration: 15m
  warmup: 2m

pipeline:
  input:
    {{CONNECTOR}}:
      # Fill in connector-specific config. For postgres_cdc / mysql_cdc, see the
      # postgres/mysql scenarios as exemplars.
      dsn: ${{{ENGINE | upper}}_DSN}
      # TLS: if your input uses NewTLSField, do NOT set `enabled:` — see traps.md#postgres-cdc-tls

matrix:
  cpu_points: [1, 2, 4, 8]

reset:
  # Drop engine-specific replication state (slot, GTID, savepoint) BEFORE truncate.
  # For postgres: pg_drop_replication_slot for both Connect AND KC slot.
  # For mysql: no slot — Plan 3's per-vCPU connector name handles KC offset isolation.
  # - sql: "..."
  - sql: "TRUNCATE TABLE {{TABLE}}"
```

- [ ] **Step 5: Write `engine-spec.go.tmpl`**

```go
// Add this entry to engineSpecs in benchmarking/aws/runner/scenario.go.
//
// Pick ONE of two patterns:
//
// PATTERN A — DSN-only (engine's CLI accepts a connection-string URL, e.g. psql):
//
//     "{{CONNECTOR}}": {
//         DSNOutputKey: "{{ENGINE}}_dsn",
//         DSNEnvVar:    "{{ENGINE | upper}}_DSN",
//     },
//
// PATTERN B — discrete fields (engine's CLI wants -h/-P/-u/-p, e.g. mysql):
//
//     "{{CONNECTOR}}": {
//         DSNOutputKey:       "{{ENGINE}}_dsn",
//         DSNEnvVar:          "{{ENGINE | upper}}_DSN",
//         ResetHostOutputKey: "{{ENGINE}}_host",
//         ResetPortOutputKey: "{{ENGINE}}_port",
//         ResetUserOutputKey: "{{ENGINE}}_user",
//         ResetPassOutputKey: "{{ENGINE}}_password",
//         ResetDBOutputKey:   "{{ENGINE}}_db",
//     },
//
// Decision rule: try the DSN form first; if the engine's CLI doesn't accept it,
// use the discrete form. See references/exemplar-tour.md.
```

- [ ] **Step 6: Write `kc-connector-spec.go.tmpl`**

```go
// Add this entry to kcConnectorSpecs in benchmarking/aws/runner/kcconnectors.go.
//
// Steps:
//   1. Pick the KC plugin from references/kc-connector-mapping.md.
//   2. If the plugin isn't already installed, add it to the runner's cloud-init
//      (see references/kc-connector-mapping.md#adding-a-new-kc-plugin-cloud-init-step).
//   3. Copy the postgres or mysql entry as a starting template and adjust.
//   4. Set snapshot.mode according to whether your engine needs an offset.
//   5. Set RequiredPlugins glob to match the installed plugin.

"{{CONNECTOR}}": {
    Class:     "<TODO: e.g. io.debezium.connector.sqlserver.SqlServerConnector>",
    Direction: kcSource,  // or kcSink
    PropsTemplate: `{
  "connector.class": "<TODO same as Class>",
  "tasks.max": "1",
  "database.hostname": "{{.Host}}",
  "database.port": "{{.Port}}",
  "database.user": "{{.User}}",
  "database.password": "{{.Password}}",
  "database.dbname": "{{.Database}}",
  "topic.prefix": "{{.TopicPrefix}}",
  "table.include.list": "{{.SchemaTables}}",
  "snapshot.mode": "<TODO: never | no_data | initial>",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
    RequiredPlugins: []string{"<TODO: e.g. debezium-connector-sqlserver*>"},
},
```

- [ ] **Step 7: Write `reset.sql.tmpl`**

```sql
-- Reset block for {{CONNECTOR}} between sweep points.
--
-- This SQL goes inside the scenario YAML's `reset:` array, one statement
-- per array entry. The runner runs these BEFORE EVERY sweep point.
--
-- Pattern for postgres-shaped engines (slot-based replication):
--
-- - sql: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"
-- - sql: "SELECT pg_drop_replication_slot('kc_bench_slot') FROM pg_replication_slots WHERE slot_name='kc_bench_slot'"
-- - sql: "TRUNCATE TABLE {{TABLE}}"
--
-- Pattern for mysql-shaped engines (binlog, no slots):
--
-- - sql: "TRUNCATE TABLE {{TABLE}}"
--
-- Pattern for SQL Server (similar to mysql — no slots, but you may need to disable+re-enable CDC):
--
-- - sql: "EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='{{TABLE}}', @capture_instance='all'"
-- - sql: "TRUNCATE TABLE {{TABLE}}"
-- - sql: "EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='{{TABLE}}', @role_name=null"
--
-- Key invariants:
-- 1. The table MUST be truncated (avoid Trap 8 — see references/traps.md#truncate-between-points)
-- 2. Both Connect AND KC replication state must be cleared (each engine may have its own slot/offset)
-- 3. For KC, the per-vCPU connector naming (in the bench script) handles offset isolation —
--    you don't need an explicit DELETE; combineReset injects connector DELETE + topic deletes.
```

- [ ] **Step 8: Verify all template files exist**

```bash
ls -la .claude/skills/bench-framework/assets/templates/
ls -la .claude/skills/bench-framework/assets/templates/tf-stack/
```

Expected: 7 template files across the two directories.

- [ ] **Step 9: Commit**

```bash
git add .claude/skills/bench-framework/assets/templates/
git commit -m "$(cat <<'EOF'
feat(skill): asset templates for new-connector scaffolding

Templates the skill renders during the Add-a-new-connector flow:
- tf-stack/{main,variables,outputs}.tf.tmpl
- scenario.yaml.tmpl
- engine-spec.go.tmpl (two patterns: DSN vs. discrete fields)
- kc-connector-spec.go.tmpl
- reset.sql.tmpl (postgres / mysql / sql-server shapes)

Placeholders: {{CONNECTOR}}, {{ENGINE}}, {{TABLE}}.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: Fill in "Add a new connector" section in SKILL.md

**Files:**
- Modify: `.claude/skills/bench-framework/SKILL.md`

- [ ] **Step 1: Replace the `## Add a new connector` placeholder**

```markdown
## Add a new connector

This walkthrough produces 5 artifacts: a TF stack, scenario YAML, engineSpec entry, kcConnectorSpec entry, and reset SQL. Plus tests. Walk top-to-bottom.

Read [references/exemplar-tour.md](references/exemplar-tour.md) FIRST — it's the mental model.

### Step 0: Interview yourself

Answer these before writing anything:

1. **Connector category?** CDC source / sink / pure-stream / other.
2. **Upstream/downstream system?** (e.g. `sqlserver_cdc`, `mongo_cdc`, `iceberg`, `s3`)
3. **For CDC:** Postgres-shaped (slot-based logical replication) or MySQL-shaped (binlog, no slots)? Determines exemplar.
4. **Connector name** in `internal/impl/<x>/`? (becomes the key in `engineSpecs` and `kcConnectorSpecs`)

### Step 1: Pick the exemplar

| New connector type | Mirror after |
|--------------------|--------------|
| Postgres-shaped CDC (logical replication, slots) | `scenarios/postgres/orders-cdc.yaml` + `kcConnectorSpecs["postgres_cdc"]` |
| MySQL-shaped CDC (binlog, no slots) | `scenarios/mysql/orders-cdc.yaml` + `kcConnectorSpecs["mysql_cdc"]` |
| SQL Server CDC (table tail, schema-changes) | Closest: mysql shape; needs `sp_cdc_enable_table` in reset (Plan 4 territory) |
| Mongo CDC (change stream) | Closest: postgres shape; Debezium MongoDB needs `mongo.connection.string` not user/pass split |
| Sink (any) | Plan 4 — TBD when sink scenarios land |

If your connector doesn't fit, read both postgres and mysql exemplars in `references/exemplar-tour.md` and synthesise.

### Step 2: Scaffold the TF stack

Create `benchmarking/aws/terraform/stacks/<connector>/` mirroring `stacks/postgres/`. Use the templates in `.claude/skills/bench-framework/assets/templates/tf-stack/`:

```bash
mkdir -p benchmarking/aws/terraform/stacks/<connector>/
# Copy each template, substituting {{CONNECTOR}} → <your connector key>
# (e.g. sqlserver_cdc) and {{ENGINE}} → <your engine name> (e.g. sqlserver).
```

You'll also need a `terraform/modules/rds-<engine>/` module if your engine isn't postgres or mysql. Mirror `modules/rds-postgres/`.

Check `references/rds-quirks.md` for any engine-specific RDS parameter group rules.

### Step 3: Write the scenario YAML

Create `benchmarking/aws/scenarios/<stack>/<scenario>.yaml` from `assets/templates/scenario.yaml.tmpl`. Pre-filled sizing defaults (instance class, storage, IOPS) match the postgres exemplar.

Fill in the connector-specific bits:
- `pipeline.input.<connector>:` block (look at the Connect docs for required fields)
- Engine-specific `infra.source.parameters` (see `references/rds-quirks.md`)
- Reset SQL inside `reset:` (see Step 6)

Apply the sizing checklist in [references/scenario-sizing.md](references/scenario-sizing.md#sizing-checklist-for-a-new-scenario).

### Step 4: Add the engineSpec entry

Edit `benchmarking/aws/runner/scenario.go::engineSpecs`. Use the template at `assets/templates/engine-spec.go.tmpl` — it documents the DSN-only vs. discrete-fields decision.

**Delegate to the `godev` agent:**

> Add this engineSpec entry to `benchmarking/aws/runner/scenario.go::engineSpecs`. Match the existing postgres or mysql pattern (whichever fits — DSN-only or discrete-fields). Preserve the license header per CLAUDE.md.

The entry from the template:

```go
// (paste your filled-in template here)
```

### Step 5: Add the kcConnectorSpec entry

Edit `benchmarking/aws/runner/kcconnectors.go::kcConnectorSpecs`. Use the template at `assets/templates/kc-connector-spec.go.tmpl`.

Critical: check [references/kc-connector-mapping.md](references/kc-connector-mapping.md) for plugin selection AND cloud-init plumbing — if the plugin isn't already installed on the runner, you'll need to add it in `terraform/shared/runner-user-data.tftpl`.

**Delegate to the `godev` agent:**

> Add this kcConnectorSpec entry to `benchmarking/aws/runner/kcconnectors.go::kcConnectorSpecs`. Mirror the postgres or mysql pattern. Set snapshot.mode according to whether the engine needs an offset to bootstrap (see references/kc-connector-mapping.md). Preserve the license header.

### Step 6: Reset SQL

In your scenario YAML's `reset:` array, write the engine-specific cleanup. See `assets/templates/reset.sql.tmpl` for postgres / mysql / SQL Server patterns.

Invariants (DO NOT skip):
- TRUNCATE the target table (Trap 8 — see [traps.md#truncate-between-points](references/traps.md#truncate-between-points))
- Drop Connect's replication slot AND KC's replication slot if both engines are being swept (postgres shape only)
- For KC, the per-vCPU connector naming in the bench script handles offset isolation — combineReset auto-injects connector DELETE + topic deletes ([traps.md#kc-connector-offset-isolation](references/traps.md#kc-connector-offset-isolation))

### Step 7: Validate

```bash
task aws:validate scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml
```

Catches typos and missing-registry errors before any AWS spend. On failure, the error message will point at the issue; cross-check against [references/traps.md](references/traps.md).

### Step 8: 1-vCPU smoke

```bash
# Pin cpu_points to [1] in your scenario for the smoke run
aws-vault exec bench -- task aws:bench \
  scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml \
  --engines connect,kafka_connect
```

Acceptance:
- Result JSON has TWO `PointResult` entries (one per engine)
- KC's `Summary.MedianMBPerSec > 0` (broker-derived)
- `BrokerSeries` populated for both engines

On failure → [Debug](#debug).

### Step 9: Tests

**Delegate to the `tester` agent:**

> Add scenario_test.go cases covering the new connector in engineSpecs AND kcConnectorSpecs. Mirror the existing test cases for postgres_cdc and mysql_cdc in `benchmarking/aws/runner/scenario_test.go` and `kcconnectors_test.go`. Run with `task test:unit -- benchmarking/aws/runner`.

### Step 10: Commit

After smoke passes and tests green:

```bash
git add benchmarking/aws/terraform/stacks/<connector>/ \
        benchmarking/aws/scenarios/<stack>/ \
        benchmarking/aws/terraform/modules/rds-<engine>/ \
        benchmarking/aws/runner/scenario.go \
        benchmarking/aws/runner/kcconnectors.go \
        benchmarking/aws/runner/scenario_test.go \
        benchmarking/aws/runner/kcconnectors_test.go

git commit -m "feat(bench): add <connector> head-to-head bench"
```
```

- [ ] **Step 2: Verify SKILL.md is still ≤ 300 lines**

```bash
wc -l .claude/skills/bench-framework/SKILL.md
```

Expected: ≤ 300 lines.

- [ ] **Step 3: Verify all `references/` and `assets/` links resolve**

```bash
# Extract all internal links from SKILL.md
grep -oE '\((references|assets)/[a-z./-]+(\.md|\.tmpl|/)?' .claude/skills/bench-framework/SKILL.md \
  | sed 's/^(//' | sort | uniq > /tmp/links-used.txt

# Check each referenced file/dir exists
while read -r path; do
  cleaned="${path%[#)]*}"  # strip anchor + trailing paren
  test -e ".claude/skills/bench-framework/$cleaned" \
    || echo "MISSING: $cleaned"
done < /tmp/links-used.txt
```

Expected: no `MISSING:` lines.

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/bench-framework/SKILL.md
git commit -m "$(cat <<'EOF'
feat(skill): add-a-new-connector section in SKILL.md

10-step walkthrough: interview, pick exemplar, scaffold TF stack
from template, write scenario YAML, add engineSpec via godev,
add kcConnectorSpec via godev, reset SQL, validate, 1-vCPU smoke,
tests via tester. References templates in assets/ and deep-dives
in references/.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase E: Verification

### Task 14: Dry-run scaffolding `sqlserver_cdc` and fix gaps

**Files:**
- Possibly modify: any SKILL.md / references / templates if dry-run surfaces gaps

End-to-end verification: pretend to be a teammate adding `sqlserver_cdc`. Walk the entire skill flow and note every place it hand-waves, points at a missing file, or can't proceed without prior knowledge.

- [ ] **Step 1: Read the skill cold**

Read `.claude/skills/bench-framework/SKILL.md` start-to-finish as if you're a Redpanda engineer who's never opened `benchmarking/aws/`. Make notes (mentally or in scratch file) every time you hit:
- A reference to a file/concept the skill doesn't explain
- A decision the skill expects you to make without enough info
- A template field that's ambiguous

- [ ] **Step 2: Walk the 10-step add-connector flow for `sqlserver_cdc`**

For each step:
1. What artifact does the skill say to produce?
2. Does the template (or pointer) give you enough to actually produce it?
3. If something's missing, write it down.

Specifically check:
- Step 1 exemplar table — is "SQL Server CDC" guidance enough? (Probably needs more detail than "Closest: mysql shape".)
- Step 2 — does the TF stack template work with a non-postgres/non-mysql engine?
- Step 5 — kc-connector-mapping says SQL Server's Debezium plugin isn't installed. Does the skill's "add a new KC plugin" instructions actually get you there?

- [ ] **Step 3: Document gaps as a checklist**

Write a `gaps.md` scratch file (do not commit) listing each gap with a category:
- **CRITICAL**: skill can't carry the flow at all (e.g. broken template, missing reference)
- **MAJOR**: skill carries the flow but leaves a 10+ minute side-quest the engineer has to figure out alone
- **MINOR**: small wording fix or a missing example

- [ ] **Step 4: Fix CRITICAL and MAJOR gaps inline**

For each CRITICAL/MAJOR gap:
- If it's a SKILL.md issue → edit SKILL.md
- If it's a reference issue → edit the matching `references/*.md`
- If it's a template issue → edit the matching `assets/templates/*.tmpl`

Defer MINOR gaps to a future polish PR — list them in commit message.

- [ ] **Step 5: Re-verify all anchor links**

```bash
# Re-run the link-resolution checks from Tasks 3, 7, 13
grep -E '^<a name="' .claude/skills/bench-framework/references/traps.md | sed 's/.*name="//;s/".*//' | sort > /tmp/anchors-defined.txt
grep -E '^<a name="' .claude/skills/bench-framework/references/debugging-playbook.md | sed 's/.*name="//;s/".*//' | sort > /tmp/playbook-defined.txt
grep -E '^<a name="' .claude/skills/bench-framework/references/scenario-sizing.md | sed 's/.*name="//;s/".*//' | sort > /tmp/sizing-defined.txt

# Aggregate all references and check
grep -roE 'traps\.md#[a-z-]+' .claude/skills/bench-framework/ | awk -F: '{print $2}' | sed 's/traps.md#//' | sort -u > /tmp/anchors-used.txt
comm -23 /tmp/anchors-used.txt /tmp/anchors-defined.txt
```

Expected: no missing anchors.

- [ ] **Step 6: Verify size budgets**

```bash
wc -l .claude/skills/bench-framework/SKILL.md
wc -l .claude/skills/bench-framework/references/*.md
```

Expected: SKILL.md ≤ 300 lines. References ≤ 200 lines each, except `traps.md` ≤ 250 lines (the load-bearing trap table is allowed to be the longest).

- [ ] **Step 7: Invoke `superpowers:writing-skills` to verify the skill loads cleanly**

Use the Skill tool to invoke `superpowers:writing-skills` against the new skill at `.claude/skills/bench-framework/`. Confirm:
- Frontmatter parses (name + description present)
- All internal links resolve
- Skill loads without errors when triggered by a test prompt mentioning `benchmarking/aws/`

Address any issues writing-skills surfaces.

- [ ] **Step 8: Commit fixes (if any)**

```bash
git add .claude/skills/bench-framework/
git commit -m "$(cat <<'EOF'
fix(skill): close gaps surfaced by sqlserver_cdc dry-run

End-to-end verification of the add-a-new-connector flow against
a hypothetical sqlserver_cdc. Patched: <list of gap categories
addressed inline>.

Deferred (minor polish): <list any minor items>.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

If no gaps surfaced, skip the commit and just note that in the agent's final summary.

---

## Self-review checklist (for the implementer)

Before declaring done, the implementer should verify:

- [ ] All 14 tasks committed in order
- [ ] `.claude/skills/bench-framework/SKILL.md` ≤ 300 lines
- [ ] Each `.claude/skills/bench-framework/references/*.md` ≤ 350 lines (most ≤ 200)
- [ ] Every anchor referenced in SKILL.md resolves to a `<a name="...">` in a references file
- [ ] All 7 templates exist with no `{{TODO}}` placeholders that should have been filled
- [ ] Frontmatter `name:` and `description:` parse correctly (verify by spot-checking the loading hook in Claude Code)
- [ ] No CI checks were added — the maintenance contract is convention-only per the spec
- [ ] Dry-run found no CRITICAL gaps

## Open items (defer; do NOT implement in this plan)

1. `task skill:verify` target that pins SKILL.md references to current Go symbols.
2. Sinks (Plan 4) coverage — exemplar table currently says "TBD".
3. Cross-linking to `redpanda-connect:pipeline` skill for Connect-side YAML inside scenarios.
4. Visual companion mockups for the decision tree.
