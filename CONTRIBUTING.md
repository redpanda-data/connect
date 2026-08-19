# Redpanda Connector Certification

Redpanda Connect supports a wide array of connectors for integrating with popular data systems. While many are community-contributed, certified connectors are officially supported by Redpanda.  
This document outlines the criteria for certification, ensuring a great user experience and sustainable supportability, while continuing to welcome high-quality community contributions.

---

## 1. Certification Overview

To certify a connector, it must meet the following requirements:

### 1.1 Clear Documentation & Good UX

- **1.1.1** Concise, well-organized documentation with configuration examples.  
- **1.1.2** Includes expected usage patterns, troubleshooting guidance, and known pitfalls.  
- **1.1.3** UX should be intuitive and require minimal explanation. Follow a “don’t make me think” philosophy.

### 1.2 Observability & Debuggability

- **1.2.1** Exposes useful metrics for debugging that avoid excessive cardinality.  
- **1.2.2** Provides relevant logging to support troubleshooting. Unexpected behavior should emit warning or error logs. Normal operation should emit no logs.  
- **1.2.3** Known limitations and edge cases are documented.  
- **1.2.4** Strongly lints and validates user-provided configuration, clearly telling users of any problems.

### 1.3 Reliability & Testing

- **1.3.1** Code is idiomatic following Effective Go recommendations, is readable, and is consistent with the broader Redpanda Connect code base.  
- **1.3.2** Tests should cover end-to-end functionality and prove that the connector works across supported configurations.  
- **1.3.3** Integration tests verify core workflows and are runnable in CI.
- **1.3.4** Benchmarking covers **two distinct phases**, both of which are required:
  - **Local benchmarking (localhost):** unit and local integration benchmarks that run without external infrastructure, giving fast, repeatable feedback during development.
  - **Real-endpoint benchmarking:** benchmarks run against a real deployed server / real endpoint, exercising the connector against the actual target system rather than a local stand-in.
  - Follow the standard process, directory layout, and reporting requirements in [`docs/benchmarking.md`](docs/benchmarking.md); record results under [`docs/benchmark-results/`](docs/benchmark-results/).
- **1.3.5** Across both phases, benchmarks have been run at various throughput levels so that we can determine CPU and memory trendlines based on usage.
- **1.3.6** If a corresponding Kafka Connect connector exists, benchmarks have been run against it so we can compare it against our throughput and ensure Redpanda Connect's is comparable or better.

---

## 2. Connector Selection Criteria

When deciding which connectors to prioritize or certify, Redpanda considers:

### 2.1 Preferred Characteristics

- **2.1.1** Integrates well with Redpanda as a company.  
- **2.1.2** Represents widely used and recognized tools in the data engineering ecosystem.  
- **2.1.3** Is well documented and has an active, engaged user base.

### 2.2 Deprioritized Characteristics

- **2.2.1** Niche, outdated, or declining technologies.  
- **2.2.2** High barriers to testing (e.g., requires proprietary infrastructure).  
- **2.2.3** Fragile, costly, or hard to operate in real-world environments.

---

## 3. Implementation Standards

We hold certified connectors to a consistent engineering bar so that they are reliable, maintainable, and supportable.

### 3.1 Required Engineering Qualities

- **3.1.1** Connector code is either authored by Redpanda engineers or reviewed and scoped by Redpanda before community contribution (e.g., defined in a GitHub issue or an internal PRD) — and the PR stays within that agreed scope; additional components or capabilities beyond it are proposed and reviewed separately.  
- **3.1.2** Code adheres to standard Go practices: idiomatic, well-structured, self-documenting, and formatted with `gofumpt` (`task fmt`) so it stays consistent with the rest of the codebase.
- **3.1.3** Connectors are written **Go-first** — idiomatic Go, never a line-by-line port of an implementation from another ecosystem (e.g. Debezium for CDC). We build toward a goal: **supporting the target endpoint well**, where "well" is defined by the rest of this document — clear documentation and UX (§1.1), well-designed configuration knobs and validation (§1.2.4), observability (§1.2), and reliability (§1.3). A reference implementation may be consulted to understand the endpoint or protocol, but it is not a specification to replicate, and we are not bound to its abstractions, idioms, or naming. Design the connector for our users and our codebase, not for parity with another tool.  
- **3.1.4** The implementation is complete and correct, with no known bugs or missing core functionality.  
- **3.1.5** The codebase feels consistent with other Redpanda Connect connectors, avoiding bespoke or idiosyncratic implementations.  
- **3.1.6** Integration tests are easy to run locally and in CI environments, ideally with containerized dependencies.  
- **3.1.7** Supports live credential rotation (e.g., for tokens or certs) with no downtime where applicable.  
- **3.1.8** Has sufficient observability: logs, metrics, and tracing hooks as expected.

### 3.2 Anti-Patterns to Avoid

- **3.2.1** Incomplete implementations.  
- **3.2.2** Poor error handling or difficult-to-diagnose bugs.  
- **3.2.3** Unfamiliar or confusing UX patterns.  
- **3.2.4** Code that is difficult to test or maintain.  
- **3.2.5** Excessive resource usage (e.g., unnecessary goroutines, memory or CPU overhead).

### 3.3 Contribution Process & Change Size

- **3.3.1** Changes are split into reviewable units. As a rule, neither a single PR nor an individual commit should reach ~10K lines of **code that a reviewer must read** — this keeps changes reviewable. AI-generated code counts in full: it needs *more* review, not less, and the limit is not a license to autogenerate past it. Only content that isn't reviewed line-by-line is excluded — mechanically generated/derived files (codegen output, mocks, bundle imports), vendored code, lockfiles, and non-code such as documentation, skills, Terraform, templates, and test fixtures/data. When a large PR is genuinely unavoidable, say why in the description and point reviewers at what matters.  
- **3.3.2** Large features are broken into a series of smaller, self-contained PRs that can each be reviewed and reasoned about independently, rather than landed as one monolithic change.  

### 3.4 Commit Conventions

These apply to every commit, whether authored by a human or an AI agent.

- **3.4.1** *Granularity.* Each commit is one small, self-contained, logical change; do not mix unrelated work in a single commit. In a multi-commit PR, documentation changes go in their own commit, separate from code.
- **3.4.2** *Message format.* The subject line must match one of:
  - `system: message` — lowercase `system` naming a known area (e.g. `otlp: add authz support`, `kafka: fix consumer group rebalance`).
  - `system(subsystem): message` — same, with a parenthesized subsystem (e.g. `gateway(authz): add http middleware`, `cli(mcp): handle shutdown`).
  - `chore: message` — low-importance cleanup, maintenance, or housekeeping (e.g. `chore: update gitignore`).
  - A sentence-case plain message for repo-wide changes not scoped to one system (e.g. `Bump to Go 1.26`, `Update CI workflows`): first word capitalized, the rest lowercase unless a proper noun.
  - `Revert "..."` and merge commits are exempt.

  In every non-exempt case the `message` starts lowercase and uses the imperative mood ("add", "fix" — not "added", "fixes"). A trailing PR-number suffix such as `(#1234)` is fine.
- **3.4.3** *Message quality.* The message accurately describes the change. Avoid vague (`fix stuff`, `updates`, `WIP`), misleading (subject does not match the diff), or incomprehensible messages.
- **3.4.4** *No fixup commits.* Squash `fixup!`/`squash!` commits before requesting review; they must not appear in the final history.

---

## 4. Client Library Evaluation

The connector’s reliability also depends on the underlying client library:

### 4.1 Preferred Traits

- **4.1.1** Maintained by the vendor of the target technology.  
- **4.1.2** Actively developed and well adopted in the Go ecosystem.  
- **4.1.3** Stable, performant, and well understood.  
- **4.1.4** Adheres to semantic versioning and is v1 or greater.

### 4.2 Red Flags

- **4.2.1** Outdated or inactive libraries.  
- **4.2.2** Known security issues or critical bugs.  
- **4.2.3** Poor runtime behavior: excessive goroutines, memory leaks, or non-linear scaling.

---

## 5. CDC Connector Standard

This standard governs **every** CDC connector — the current fleet (`oracledb_cdc`, `microsoft_sql_server_cdc`, `mysql_cdc`, `postgres_cdc`, `mongodb_cdc`, `aws_dynamodb_cdc`, `cockroachdb_changefeed`, `salesforce_cdc`, `gcp_spanner_cdc`, `tigerbeetle_cdc`) and every new one. Existing connectors may retain prior behavior where conforming would be a breaking change; **new CDC connectors must conform from the start**, enforced by the conformance test at `internal/plugins/cdctest`. Requirements are tiered: **Core** applies to every CDC connector; **Relational** applies to snapshot + change-log connectors.

The rule throughout is **conformance to the existing fleet**: mirror the shape the fleet already establishes (referenced per item) rather than introducing a new shape or porting another ecosystem's abstractions (see §3.1.3).

### 5.1 Registration & naming

- **5.1.1** A CDC input is registered as `<system>_cdc` (or `<system>_changefeed` for changefeed-style sources). This naming is the fleet and tooling contract and is required.

### 5.2 Message shape (Core)

- **5.2.1** Emit **flat, top-level metadata** plus the **raw row/document as the message body**. Do **not** wrap the row in a nested `before`/`after`/`source`/`op` envelope. A foreign-compatible output format may be offered as an explicit opt-in, but the default and internal representation is the flat fleet shape. Reference: `oracledb_cdc`, `mysql_cdc`, `postgres_cdc`.
- **5.2.2** Required metadata keys (use the fleet names; do not invent component-prefixed variants): `operation`; `schema` (immutable; relational — see §5.5.1); a source-position key under its DB-native name (`scn`, `lsn`, `binlog_position`, …); table identity `table` and namespace `database_schema`; `source_ts_ms` and `commit_ts_ms` (int64); `transaction_id`.

### 5.3 Configuration naming (Core)

- **5.3.1** Use the canonical field names: `tables` (or `include`/`exclude`), `stream_snapshot`, `snapshot_max_batch_size`, `max_parallel_snapshot_tables`, `checkpoint_cache`, `checkpoint_cache_key`, `checkpoint_limit`, `heartbeat_interval`, `batching`. `snapshot_mode` (enum) is acceptable only where a connector genuinely needs more than two snapshot modes.
- **5.3.2** Renaming an existing field is non-breaking: add the canonical field, keep the old one accepted, and mark the old one `Deprecated()`. Precedent: `postgres_cdc`'s `snapshot_memory_safety_factor`.
- **5.3.3** Do not invent bespoke names where a canonical one already exists. Common non-canonical names and their canonical replacements: `enable_snapshot` → `stream_snapshot`; `cursor_cache` → `checkpoint_cache`; `checkpoint_key` → `checkpoint_cache_key`; `snapshot_batch_size` → `snapshot_max_batch_size`; `snapshot_parallelism`, `max_parallel_snapshot_objects`, `snapshot_segments` → `max_parallel_snapshot_tables`. New connectors must use the canonical names from the start; the conformance test at `internal/plugins/cdctest` enforces this.

### 5.4 Core requirements (every CDC connector)

- **5.4.1** Durable checkpoint + resume via a `checkpoint_cache` resource; never an in-memory-only cursor. Checkpoint **as soon as the snapshot completes**, not after the first streaming message. Reference: `oracledb_cdc`. A connector that provides equivalent durability through a native or self-managed checkpoint store — e.g. `postgres_cdc`'s replication slot, or `aws_dynamodb_cdc`'s self-managed checkpoint table (auto-created, optionally a multi-region global table) — satisfies this requirement without a `checkpoint_cache` resource and is recorded as a permanent waiver (not a migration TODO) in the conformance test.
- **5.4.2** At-least-once delivery, with progress gated on downstream ack and correct transaction boundaries.
- **5.4.3** A `ConnectionTest` validating connectivity, auth, and minimum privilege before the input starts.
- **5.4.4** Tracing spans around the snapshot, stream, and checkpoint-commit paths.
- **5.4.5** TLS + auth, with IAM where the endpoint supports it; on connection loss, reconnect with freshly-resolved credentials.
- **5.4.6** Deterministic type mapping; emit `DECIMAL`/`NUMERIC` as canonical decimal strings via `sqlutil.CanonicaliseDecimal`, never `float64`. Reference: `mysql_cdc`.
- **5.4.7** Benchmarking per §1.3.4–1.3.5.

### 5.5 Relational requirements (snapshot + change-log connectors)

- **5.5.1** `schema` metadata derived from the system catalog, including primary keys and per-column nullability, with addition-only drift detection. Reference: `oracledb_cdc`, `postgres_cdc`.
- **5.5.2** Snapshot + streaming with a gap-free handoff: capture the stream position before the snapshot read and resume streaming from it.
- **5.5.3** Parallel snapshotting (`max_parallel_snapshot_tables`).
- **5.5.4** Incremental, signal-driven snapshotting.
- **5.5.5** Snapshot query filtering.
- **5.5.6** Idle keepalive that advances the read position/watermark during idle (`heartbeat_interval`). Reference: `postgres_cdc`.
- **5.5.7** Include/exclude regex table matching. Reference: `oracledb_cdc`.

### 5.6 Testing (extends §1.3)

- **5.6.1** Snapshot↔streaming type parity: every supported type serializes identically whether produced by snapshot or by streaming, proven by unit **and** integration tests.

---

## 6. Before You Open a PR

- Run `task fmt`, `task lint`, and `task test` locally — all green.
- Run `task docs` and commit the result: the generated component pages **and** the `internal/plugins/info.csv` row. CI fails on stale docs.
- Every new component has an `internal/plugins/info.csv` entry with the correct distribution and cloud classification.
- A license header on **every** new `.go` file (including test and benchmark helpers), matching the component's distribution.
- No binaries, build artifacts, or local tooling committed.
- Keep the PR within the scope agreed in the issue or PRD (§3.1.1); propose extra components or capabilities separately.
