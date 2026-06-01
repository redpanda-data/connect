# Bench Framework Claude Skill — Design

**Date:** 2026-05-31
**Author:** Prakhar Garg
**Status:** Approved (pending plan)

## Summary

Package the AWS bench framework (`benchmarking/aws/`) as an in-repo Claude
skill at `.claude/skills/bench-framework/` so that any Redpanda Connect
engineer can add a new connector benchmark — or run an existing one —
without re-learning the whole framework. The skill auto-loads when context
matches bench work and walks the engineer through scaffolding, operating,
and debugging.

The framework currently lives in `benchmarking/aws/` and supports a
Connect-vs-Kafka-Connect head-to-head comparison via per-engine sweeps
(see `kc-comparison-state` memory + the four `kafka-connect-plan-*`
spec/plan files). Two reference connectors exist: `postgres_cdc` and
`mysql_cdc`. The hard-won knowledge — 16+ traps that bit during Plans
1–3 — currently lives in private memory files. The skill makes that
knowledge first-class and shareable.

## Goals

1. A teammate hitting `benchmarking/aws/` for the first time can add a
   new CDC connector bench (TF stack + scenario YAML + engineSpec +
   kcConnectorSpec + reset SQL) by following the skill's walkthrough,
   referencing the existing `postgres_cdc` / `mysql_cdc` exemplars.
2. A teammate running an existing bench gets a workflow checklist that
   surfaces the non-obvious traps (aws-vault wrapping, license placement,
   SIGINT teardown, 4h orphan TTL) before they spend money.
3. A teammate debugging a failed bench gets a symptom-driven lookup
   into the known failure modes from Plans 1–3, each pointing at the
   commit SHA that fixed it.

## Non-goals (v1)

- Sink connectors (Plan 4 hasn't landed). Templates and exemplar
  pointers say "TBD when Plan 4 lands" rather than guessing the shape.
- Executing AWS commands. The skill describes commands; the operator
  runs them. AWS spend stays gated by human approval.
- Cross-linking to the existing `redpanda-connect:pipeline` skill for
  the Connect-side YAML inside scenarios. Nice-to-have, defer.
- CI-enforced symbol pinning between skill content and `runner/` Go
  files. The skill spec calls out the maintenance contract (Section
  "Maintenance"); CI enforcement is a follow-up.
- A separate "operate-only" skill. One skill covers add + operate +
  debug. Tradeoff accepted in brainstorming: in practice these flows
  are continuous (engineer adds a connector, then runs it, then debugs
  it).

## Approach (decided in brainstorming)

| Decision | Outcome |
|----------|---------|
| Distribution | In-repo `.claude/skills/bench-framework/` (auto-loads when working in `benchmarking/aws/`) |
| Scope | Add new connector + operate existing + debug failures (single skill) |
| Activity level | Scaffold & guide — skill renders templates and walks engineer through fill-in |
| Audience | Connect-experienced, bench-new |

## Layout

```
.claude/skills/bench-framework/
├── SKILL.md                        # Top-level decision tree + 3 branching workflows (~250 lines)
├── references/
│   ├── traps.md                    # 16-row trap table, anchored per row
│   ├── rds-quirks.md               # RDS Postgres params, postgres_cdc TLS, instance minimums
│   ├── kc-connector-mapping.md     # Debezium vs. Aiven JDBC decision tree
│   ├── scenario-sizing.md          # workload --rate, seeder concurrency, TRUNCATE, ceilings
│   ├── workflow-essentials.md      # aws-vault, license path, SIGINT, --keep-on-fail
│   ├── debugging-playbook.md       # Symptom → root cause walks
│   └── exemplar-tour.md            # Annotated walk of postgres + mysql_cdc files
└── assets/
    └── templates/
        ├── tf-stack/               # Skeleton TF stack (variables.tf, outputs.tf, main.tf)
        ├── scenario.yaml.tmpl      # Scenario YAML stub with placeholders
        ├── engine-spec.go.tmpl     # engineSpec entry stub
        ├── kc-connector-spec.go.tmpl # kcConnectorSpec entry stub
        └── reset.sql.tmpl          # Reset SQL skeleton
```

Principles:

- `SKILL.md` is lean (branching + checklists). Depth lives in `references/`,
  loaded only when linked.
- Templates are real files (not embedded strings) so they can be edited
  and reviewed like any code.
- No actual Go/Terraform code lives in the skill itself; the skill
  points at the existing exemplars (`scenarios/postgres/`,
  `runner/scenario.go::engineSpecs`).

## SKILL.md design

**Frontmatter:**

```yaml
---
name: bench-framework
description: Use when working in benchmarking/aws/ on the Redpanda Connect benchmarking framework — adding a new connector bench (TF stack + scenario + engineSpec + kcConnectorSpec), running an existing bench, or debugging a bench failure. Covers both the Connect-vs-Kafka-Connect comparison framework and operational essentials (aws-vault, license placement, SIGINT teardown).
---
```

**Body structure:**

1. **Pre-flight: trap table summary** (~20 lines). One-line entry per trap
   from `references/traps.md`, each with an anchor link. Always rendered
   regardless of branch — the cost of scrolling past is negligible
   compared to a $40 failed real bench (postgres + mysql experience).

2. **Decision tree:**
   ```
   What are you doing?
     1. Adding a new connector bench    → "Add a new connector" section
     2. Running an existing bench       → "Operate a bench" section
     3. Debugging a failed bench        → "Debug" section
   ```

3. **Three H2 sections, one per branch.**

## Branch 1: Add a new connector

Nine steps, walked in order. Each step says what to do, where the
template lives, and which exemplar to reference.

**Step 0 — Interview.** Skill asks:
- Connector category? (CDC source / sink / pure-stream / other)
- Upstream/downstream system? (e.g., `sqlserver_cdc`, `iceberg`, `s3`)
- For CDC: Postgres-shaped or MySQL-shaped? (picks exemplar)
- Connector name in `internal/impl/<x>/`?

**Step 1 — Pick the exemplar.** Decision table:

| New connector type | Mirror after | Why |
|--------------------|--------------|-----|
| Postgres-shaped CDC | `scenarios/postgres/orders-cdc.yaml` + `kcConnectorSpec[postgres_cdc]` | logical replication, psql reset, pgoutput |
| MySQL-shaped CDC | `scenarios/mysql/orders-cdc.yaml` + `kcConnectorSpec[mysql_cdc]` | binlog, mysql CLI reset, `snapshot.mode=no_data` trap |
| Sink (Plan 4) | TBD when Plan 4 lands | placeholder |

**Step 2 — Scaffold the TF stack** at `benchmarking/aws/terraform/stacks/<connector>/`.
- Skill copies `assets/templates/tf-stack/` and substitutes `{{CONNECTOR}}`.
- Engineer adds connector-specific bits (RDS parameter group entries,
  new SGs, etc.). Skill points at `modules/rds-postgres/` if CDC, or
  hints at S3/SQS modules for sinks (TBD).

**Step 3 — Write the scenario YAML** at `benchmarking/aws/scenarios/<stack>/<scenario>.yaml`.
- Skill renders `assets/templates/scenario.yaml.tmpl` pre-filled with
  sizing defaults from the chosen exemplar.
- Inline comments call out sizing traps: `write_rate_per_sec` is
  per-table-per-sec NOT total; initial_rows: 0 + TRUNCATE between
  sweep points; `cpu_points` shape.

**Step 4 — Add `engineSpec` entry** in `runner/scenario.go::engineSpecs`.
- Skill renders `assets/templates/engine-spec.go.tmpl`.
- Delegates to `godev` agent: "add this engineSpec to scenario.go::engineSpecs,
  matching the postgres/mysql pattern. License header per CLAUDE.md."

**Step 5 — Add `kcConnectorSpec` entry** in `runner/kcconnectors.go::kcConnectorSpecs`.
- Skill renders `assets/templates/kc-connector-spec.go.tmpl`.
- Skill consults `references/kc-connector-mapping.md` to pick the KC
  plugin (Debezium SQL Server vs. Aiven JDBC vs. other).
- Delegates to `godev` agent same shape as Step 4.

**Step 6 — Reset SQL/script** (the scenario YAML's `reset:` block).
- Skill walks engineer through: drop replication slot (postgres) or
  skip (mysql); TRUNCATE the target table; set `snapshot.mode`
  per-engine.
- Inline link to `traps.md#kc-connector-offset-isolation` for the
  per-vCPU connector name trap (`bench_<conn>_v<N>`).

**Step 7 — Validation gate.** `task aws:validate scenario=...`.
- Read-only, no AWS spend. Skill runs this for the engineer.
- On failure: parse error, point at the matching reference.

**Step 8 — 1-vCPU smoke.** `aws-vault exec bench -- task aws:bench scenario=... cpu_points=[1]`.
- Skill describes; operator executes.
- Acceptance: result JSON has both engines' PointResults, KC
  `Summary.MedianMBPerSec > 0`, `broker_series` populated for both
  engines.

**Step 9 — Test additions.**
- Skill delegates to `tester` agent: "add scenario_test.go cases
  covering the new connector type in engineSpecs and kcConnectorSpecs."

## Branch 2: Operate a bench

**Step 1 — Pre-flight checklist:**
- aws-vault profile `bench` configured? (account 211125438402)
  → `references/workflow-essentials.md`
- Redpanda Connect Enterprise license at repo root (NOT `~/Downloads/`)?
- On `benchmarking` branch with latest commits?
- `make zip` run inside `benchmarking/aws/cleanup-lambda/` recently?

**Step 2 — Pick scenario.** Skill lists existing scenarios. Surfaces
the wall-clock estimate (~25 min for 1-vCPU smoke, ~2.5–3h for full
4-point sweep × 2 engines) and spend estimate (~$1.50 smoke, ~$8 full).

**Step 3 — Validate.** `task aws:validate scenario=...`. Read-only.

**Step 4 — Run.** `aws-vault exec bench -- task aws:bench scenario=... [--engines connect,kafka_connect]`.
- Skill explains engine-inner-loop wall-clock (KC roughly doubles
  per-point time).
- Surfaces `--keep-on-fail` so engineer can debug live without
  destroying infra.

**Step 5 — Live monitoring.**
- Heartbeat every 60s shows rolling-stats; "good" is steady ≥20 MB/s
  post-warmup.
- S3 paths: `s3://<bucket>/runs/<sess>/{sweep,prom,redpanda-{connect,kc}}-<vcpu>.{log,txt}`.
- **SIGINT (Ctrl+C), not SIGKILL** — clean teardown depends on
  `defer destroy`.

**Step 6 — Teardown.**
- Happy: bench finishes, `defer destroy` runs.
- Hung: `aws-vault exec bench -- task aws:down` (manual destroy).
- Orphan-cleanup Lambda backstops at 4h TTL. Skill warns if scenario
  wall-clock approaches that boundary.

**Step 7 — Inspect results.**
- JSON at `benchmarking/aws/results/<connector>/<scenario>/<timestamp>.json`.
- Auto-refreshed `SUMMARY.md` table (marker-bounded section).
- Per-run markdown at adjacent `.md` files.
- Skill explains the columns: `Connect p50`, `KC p50`, `broker MB/s`,
  Δ-vs-Connect, `⚠` divergence flag.

**Step 8 — On failure** → branches to Branch 3 (Debug).

## Branch 3: Debug

Symptom-driven lookup table. Each row links into `references/debugging-playbook.md`.

| Symptom | Likely cause | Reference |
|---------|-------------|-----------|
| Connect `Summary.MedianMBPerSec=0`, no `[load]` lines | Workload subcommand silent | `debugging-playbook.md#empty-workload` |
| KC HTTP 500 with no body | curl `--fail` suppressed body | `debugging-playbook.md#kc-http-500` |
| `broker_series` empty though KC log shows writes | Per-broker metric exposition or wrong label | `debugging-playbook.md#broker-series-empty` |
| `auto_create_topics` errors | Redpanda default is FALSE | `debugging-playbook.md#auto-create` |
| Connector resumes at stale LSN | Per-vCPU connector name missing | `debugging-playbook.md#offset-isolation` |
| RDS DNS timeout mid-bench | VPC DNS / network state flake | `debugging-playbook.md#dns-flake` |
| Bench hangs after orphan-cleanup | 4h TTL trip | `debugging-playbook.md#orphan-ttl` |

Each playbook entry ends with a "what to commit" pointer to the
existing fix SHA.

## References (file-by-file)

**`references/traps.md`** — The 16-row trap table from
`bench-skill-plan` memory, each row expanded with: symptom, where it
bit (Plan/smoke), fix commit SHA, prevention recipe. One anchor per
row so SKILL.md and `debugging-playbook.md` can deep-link.

Initial row set (from memory `bench-skill-plan` + `kc-comparison-state`):

1. RDS Postgres parameter rules (`rds.logical_replication`, `max_wal_senders`)
2. `postgres_cdc` TLS field shape (NewTLSField, no `enabled` toggle)
3. RDS instance class minimums for sustained CDC
4. Workload `--rate` is per-table-per-sec, not total
5. TRUNCATE between sweep points required
6. Redpanda `auto_create_topics_enabled=false` default (opposite of Apache Kafka)
7. `redpanda_topic` label name (not `topic`)
8. Per-broker metric exposition (scrape ALL brokers)
9. Per-vCPU KC connector name (`bench_<conn>_v<N>`) for Debezium offset isolation
10. MySQL Debezium needs `snapshot.mode=no_data` (not `never`)
11. `chrt --fifo` deadlocks JVM under single-core taskset
12. Cloud-init can't share between runner + load-gen (KC split-brain)
13. Orphan-cleanup Lambda's 4h TTL trips long benches
14. `bench_session_id` is NOT a TF output; inject in Go
15. SIGINT (not SIGKILL/TaskStop) for clean teardown
16. aws-vault wrapping mandatory (not `AWS_PROFILE`)
17. License file at repo root, NOT `~/Downloads/`

**`references/rds-quirks.md`** — RDS Postgres parameter rules,
`postgres_cdc` TLS field shape, RDS instance class minimums. Pulled
from `bench-rds-quirks` memory.

**`references/kc-connector-mapping.md`** — Decision tree for picking
the KC plugin per new connector type. Lists installed plugins
(Debezium PG/MySQL, Aiven JDBC sink/source) and how to extend via
cloud-init (`/opt/kafka/plugins/`).

**`references/scenario-sizing.md`** — Three sizing traps and the
postgres/mysql actual ceilings (~50K vs. ~75K msg/sec on
`db.r6g.2xlarge`). Pulled from `bench-scenario-sizing-lessons` memory.

**`references/workflow-essentials.md`** — aws-vault wrapping, license
at repo root, SIGINT teardown, `--keep-on-fail`. Pulled from
`bench-workflow-essentials` memory.

**`references/debugging-playbook.md`** — Symptom → root cause walks
for the failure modes in the Debug table. Each ends with a
"what-to-commit" pointer to the existing fix SHA. Pulled from
`bench-debugging-history` memory + the kc-comparison Plan 3 smoke
narratives.

**`references/exemplar-tour.md`** — Annotated file-by-file walk of
postgres + mysql_cdc artifacts. Trimmed from `bench-framework-code-map`
memory to just bench-relevant files (the postgres scenario, mysql
scenario, the two engineSpec entries, the two kcConnectorSpec entries,
the reset SQL inside each scenario YAML).

## Assets / templates (file-by-file)

**`assets/templates/tf-stack/`** — Directory tree mirroring
`terraform/stacks/postgres/`. Files: `main.tf` (composes shared +
module), `variables.tf`, `outputs.tf`. Placeholders: `{{CONNECTOR}}`,
`{{ENGINE}}`. Substitution happens at scaffold time.

**`assets/templates/scenario.yaml.tmpl`** — Full scenario YAML with
all required fields, sizing defaults from postgres exemplar, inline
comments calling out sizing traps.

**`assets/templates/engine-spec.go.tmpl`** — Single entry stub for
the `engineSpecs` map. Shows env var name, DSN output key, reset CLI
form fields. Engineer fills in connector-specific values.

**`assets/templates/kc-connector-spec.go.tmpl`** — Same shape for
`kcConnectorSpecs`. Includes Debezium-vs-Aiven decision comment at top.

**`assets/templates/reset.sql.tmpl`** — TRUNCATE + slot-drop skeleton.
MySQL variant notes the `snapshot.mode=no_data` trap inline.

## Maintenance contract

Three classes of content, three contracts. **All three are conventions
only in v1 — no automated enforcement. The implementer should NOT
build CI checks, lint targets, or symbol-pinning automation as part
of this work.**

1. **Pointers to source files** (`runner/scenario.go::engineSpecs`, etc.)
   - Drifts if functions are renamed. Manual fix when drift surfaces;
     `task skill:verify` target is a separate follow-up.

2. **Templates in `assets/`** (TF stack, scenario YAML, engineSpec stub)
   - Bind to current struct shapes. Convention: any PR that touches
     `runner/scenario.go::engineSpecs` or
     `runner/kcconnectors.go::kcConnectorSpecs` must also touch the
     matching template in `.claude/skills/bench-framework/assets/templates/`.
     Recorded in SKILL.md and the spec; not gated in CI.

3. **Trap table + references** (append-only knowledge)
   - New trap → new anchor in `traps.md`, new row in SKILL.md summary
     table. No churn elsewhere. Cheapest to maintain.

## Verification gate (before shipping the skill)

1. Use `superpowers:writing-skills` to verify the skill loads correctly
   (frontmatter shape, references resolvable, no broken anchors).
2. **One end-to-end dry run.** Pretend to add a hypothetical
   `sqlserver_cdc` connector and walk the full add-new-connector
   branch. If the skill can't carry that without hand-waving, it has
   gaps. Acceptance: the 9-step flow produces a buildable (not
   runnable — Sql Server stack doesn't exist) set of files that
   compile and pass `task aws:validate`.

## Out of scope (called out for follow-ups)

- Sink connectors (Plan 4). Placeholders in exemplar table say "TBD".
- CI-enforced symbol pinning between skill content and `runner/` Go
  files. Follow-up: `task skill:verify` target.
- Cross-linking to `redpanda-connect:pipeline` for Connect-side YAML
  inside scenarios.
- A separate "operate-only" skill. Single skill v1; revisit if SKILL.md
  grows beyond ~400 lines or if "operate" branch dominates real usage.

## Source memories drawn on

- `bench-skill-plan` — initial sketch + 16-trap table
- `bench-framework-code-map` — file-by-file reference for exemplar tour
- `kc-comparison-state` — KC plumbing, Plan 1–3 smoke history, fix SHAs
- `bench-workflow-essentials` — workflow non-negotiables
- `bench-rds-quirks` — RDS-specific gotchas
- `bench-scenario-sizing-lessons` — three sizing traps
- `bench-debugging-history` — full debugging playbook source

## Acceptance criteria

1. Skill loads via `superpowers:writing-skills` verification.
2. Dry-run scaffolding a hypothetical `sqlserver_cdc` connector
   produces a complete set of files matching the layout of the
   postgres exemplar (TF stack + scenario YAML + engineSpec entry +
   kcConnectorSpec entry + reset SQL).
3. All 16+ traps from source memories appear once in `traps.md` with
   anchors, and the SKILL.md summary table links to each.
4. SKILL.md is ≤ 300 lines; each `references/<file>.md` is ≤ 200 lines.
