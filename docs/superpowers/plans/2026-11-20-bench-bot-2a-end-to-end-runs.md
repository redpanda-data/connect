# Bench Bot Part 2a: End-to-End Runs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a customer's whole Connect config (input → processors → output) at 1/2/4/8 vCPU in the bench account. Use real infrastructure where we have it and approximate the rest with explicit caveats. Write the result to the catalogue, all driven by a `plan_id` through one GitHub Actions workflow.

**Architecture:**
- A new `plan` package turns a customer config plus workload into a stored Plan. It classifies the input and output, swaps identity for bench infrastructure, stubs external calls and collects caveats.
- The Plan renders to a runner scenario with a new `direction: e2e`.
- The runner gains an `e2eTopology`. It measures at the output with the harness `benchmark` processor, provisions per-run scratch in real sinks, and tears it down afterwards.
- A `bench_bot.yml` workflow fetches the plan, waits for the shared stack under an S3 lock, runs it, and sends a completion message.

**Tech Stack:** Go (module `github.com/redpanda-data/connect/v4/benchmarking/aws`), testify, aws-sdk-go-v2 (S3, Secrets Manager, Glue), `cloud.google.com/go/bigquery`, `github.com/snowflakedb/gosnowflake` (both already in the root module, so check `go.mod` before adding), Terraform, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-20-bench-bot-2-agent-design.md` §3 and §7, plus the parent `docs/superpowers/specs/2026-11-20-bench-bot-design.md`.

## Global Constraints

- New `.go` files carry the RCL header from `benchmarking/aws/runner/summary.go` lines 1–7.
- `task lint`/`task fmt` skip this module. Run `go vet ./...` and `~/go/bin/gofumpt -l catalog runner cmd plan` from `benchmarking/aws` by hand. `go mod tidy` must leave no diff.
- No unit test touches AWS, Snowflake or BigQuery. Every external call goes behind an interface with an in-memory fake.
- Fresh runs build latest `main`. Entries whose `connect_version` has a `-N-g<sha>` suffix are **unreleased** and carry caveat `unreleased_build`.
- Sweep is `cpu_points: [1, 2, 4, 8]`, with `infra.memory_per_vcpu_bytes: 4000000000` on every bot scenario.
- Caveat strings, exactly: `source_approximated`, `sink_not_measured`, `external_calls_stubbed`, `snapshot_not_measured`, `unreleased_build`, `memory_not_limited`.
- **CDC inputs without a bench stack are not runnable.** `plan.Build` returns `Runnable: false` with reason `no bench stack for <input> yet`. Approximation applies to non-CDC inputs only, i.e. where `catalog.ModeFor(input) == "bulk"`.
- Per-run scratch names are `bench_<session>` (lower-case, `[a-z0-9_]`). Every scratch object is removed in deferred teardown, and the reaper sweeps leftovers older than 4 hours.
- Customer secrets never reach a Plan, a scenario, a log line or the catalogue. The identity swap drops them before a Plan is stored.

## Review Focus

1. **A config with credentials or hosts inline.** Expect a Plan, a rendered scenario and test logs with no trace of them. Test in Task 1, scanning the marshalled Plan for every fixture secret.
2. **An unsupported CDC input (`oracledb_cdc`, `aws_dynamodb_cdc`).** Expect `Runnable: false` with the reason, never an approximated run. Test in Task 1.
3. **The sink is the bottleneck.** Throughput must be measured after the output's backpressure, not at the input. Expect the summary to come from the `benchmark` processor appended last in `output.processors`. Test in Task 3.
4. **A teardown failure, or a run killed mid-sweep.** Expect no orphaned Snowflake schema or BigQuery dataset beyond the reaper TTL. Tests in Tasks 6–8, on the reaper's sweep selection.
5. **Two approvals or two queued runs.** Expect only one run to hold the shared stack. The S3 lock uses a conditional create, and a second acquirer waits. Test in Task 9, on the lock client with a fake.

---

### Task 1: `plan` package — classify, swap identity, caveats

**Files:**
- Create: `benchmarking/aws/plan/plan.go`, `benchmarking/aws/plan/classify.go`, `benchmarking/aws/plan/identity.go`
- Test: `benchmarking/aws/plan/plan_test.go`, `benchmarking/aws/plan/identity_test.go`

**Interfaces:**
- Consumes: `catalog.Components`, `catalog.ModeFor`, `catalog.Fingerprint`.
- Produces:
  ```go
  type SourceKind string // "postgres" | "mysql" | "redpanda" | "approximated"
  type SinkKind   string // "redpanda" | "s3" | "iceberg" | "snowflake" | "bigquery" | "drop"
  type Workload struct { MessageSizeBytes int; TargetMBps float64; Mode string }
  type Plan struct {
      ID           string         `json:"plan_id"`        // random 12 hex chars
      CreatedAt    time.Time      `json:"created_at"`
      Runnable     bool           `json:"runnable"`
      Reason       string         `json:"reason,omitempty"`
      Input        string         `json:"input"`          // customer component names
      Processors   []string       `json:"processors"`
      Output       string         `json:"output"`
      Source       SourceKind     `json:"source"`
      Sink         SinkKind       `json:"sink"`
      SinkSize     string         `json:"sink_size,omitempty"`   // Snowflake warehouse size, default "XSMALL"
      Pipeline     map[string]any `json:"pipeline"`       // identity-swapped customer pipeline (placeholders, no secrets)
      Workload     Workload       `json:"workload"`
      Caveats      []string       `json:"caveats"`
      Slack        SlackRef       `json:"slack"`          // channel, thread_ts
      Estimate     catalog.Estimate `json:"estimate"`     // Task 10
      DispatchedRunID int64       `json:"dispatched_run_id,omitempty"`
  }
  func Build(config map[string]any, w Workload, slack SlackRef, now time.Time) (Plan, error)
  ```
- **Classification tables** are exported vars, so they're reviewable in one place:
  - `RealSources`: `postgres_cdc` → postgres; `mysql_cdc` → mysql; `kafka_franz`, `kafka`, `redpanda`, `redpanda_common` → redpanda.
  - `RealSinks`: `redpanda`, `kafka_franz`, `kafka` → redpanda; `aws_s3` → s3; `iceberg` → iceberg; `snowflake_streaming`, `snowflake_put` → snowflake; `gcp_bigquery` → bigquery.
  - Any other input with `ModeFor == "cdc"`: `Runnable: false`, reason `no bench stack for <input> yet`.
  - Any other input: `Source: approximated` plus caveat `source_approximated`.
  - Any other output: `Sink: drop` plus caveat `sink_not_measured`.
- **Identity swap** (`identity.go`, `func SwapIdentity(pipeline map[string]any, src SourceKind, sink SinkKind) (map[string]any, []string, error)`, returning caveats):
  - At any depth, these keys are deleted: `dsn`, `url`, `urls`, `addresses`, `seed_brokers`, `tls`, `sasl`, `auth`, `credentials`, `credentials_json`, `password`, `user`, `username`, `private_key`, `private_key_file`, `private_key_pass`, `account`, `role`, `database`, `schema`, `tables`, `table`, `topics`, `topic`, `slot_name`, `bucket`, `path`, `project`, `dataset`, `namespace`, `catalog`.
  - Then the bench's own values are set from a per-kind template using runner placeholders. Examples: `${POSTGRES_DSN}`, `${BENCH_REDPANDA_BROKERS}`, `${BENCH_TOPIC}`, `${BENCH_SNOWFLAKE_ACCOUNT}`, `${BENCH_BQ_PROJECT}`, `${BENCH_BQ_DATASET}`, `${BENCH_S3_BUCKET}`, `${BENCH_S3_PREFIX}`, `${BENCH_ICEBERG_CATALOG_URL}`, `${BENCH_ICEBERG_NAMESPACE}`.
  - Processors named `http`, `sql_raw`, `sql_select`, `sql_insert`, `cache`, `redis`, `aws_lambda`, `workflow`, or `branch` with an external call inside, are replaced with `mapping: 'root = this'`, plus caveat `external_calls_stubbed`.
  - `stream_snapshot: true` on a CDC input is set to `false`, plus caveat `snapshot_not_measured`.
- The customer's own `input`/`output` performance keys are kept untouched: anything not deleted above.

- [ ] **Step 1: Write failing tests**
  - `TestBuildPostgresToSnowflake`: Source postgres, Sink snowflake, Runnable, no caveats. `Pipeline.input.postgres_cdc.dsn == "${POSTGRES_DSN}"`. The customer's `batching.count` is preserved.
  - `TestBuildUnsupportedCDC`: `oracledb_cdc` and `aws_dynamodb_cdc` give Runnable false, with reason `no bench stack for oracledb_cdc yet`.
  - `TestBuildApproximatesBulkInput`: `http_server` input gives Source approximated, plus caveat `source_approximated`.
  - `TestBuildFallbackSink`: `elasticsearch_v8` output gives Sink drop, caveat `sink_not_measured`, and output `{drop:{}}`.
  - `TestBuildStubsExternalCalls`: an `http` processor becomes a mapping, plus caveat `external_calls_stubbed`. A pure `mapping` processor is unchanged.
  - `TestBuildSnapshotCaveat`.
  - `TestBuildNeverLeaksSecrets` (Review Focus 1): the fixture has a password in the DSN, a Snowflake private key, BigQuery `credentials_json`, SASL and a hostname. The JSON-marshalled Plan contains none of the fixture's secret strings or hosts.
  - `TestBuildIDUnique`: two Builds give different IDs matching `^[0-9a-f]{12}$`.
- [ ] **Step 2: Run** `go test ./plan/ -v` (from `benchmarking/aws`). Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expect PASS. Then `go vet ./plan/...` and gofumpt.
- [ ] **Step 5: Commit** `bench(plan): classify customer pipelines and swap identity for bench infra`.

### Task 2: Render a Plan to a runner scenario, plus `cmd/benchplan`

**Files:**
- Create: `benchmarking/aws/plan/render.go`, `benchmarking/aws/cmd/benchplan/main.go`
- Test: `benchmarking/aws/plan/render_test.go`, `benchmarking/aws/cmd/benchplan/main_test.go`, and `benchmarking/aws/runner/e2e_scenario_test.go` (it loads the golden output through `LoadScenario`)

**Interfaces:**
- Consumes: `Plan` (Task 1).
- Produces:
  - `func Render(p Plan) ([]byte, error)` returns scenario YAML with these fields:
    - `name: bot-<plan_id>`, `direction: e2e`
    - `connector: <input>`
    - `stack:`: `postgres` or `mysql` for those sources, else `none`
    - `infra.memory_per_vcpu_bytes: 4000000000`
    - `dataset.row_size_bytes: <MessageSizeBytes>`
    - `workload` from the base scenario of that stack, or for redpanda/approximated sources `preload_messages`, sized so the sweep never drains (see Task 4)
    - `pipeline`: the Plan's pipeline
    - `sink: {kind, size}`
    - `matrix.cpu_points: [1,2,4,8]`
    - `reset` per source kind
  - Errors on `!p.Runnable`.
  - CLI: `benchplan render --plan <s3://…|file> --out <dir>` writes `<dir>/bot-<id>.yaml` and prints its path. `benchplan show --plan …` prints the Plan JSON. Exit codes: 0 ok, 1 runtime error, 2 bad flags.
- Runner: scenario gains `Sink *SinkSpec \`yaml:"sink,omitempty"\`` (`Kind`, `Size`) and `PreloadMessages int64 \`yaml:"preload_messages,omitempty"\`` under `dataset`. Validation:
  - `direction: e2e` requires a `sink`.
  - `stack: none` is allowed only for e2e.
  - `preload_messages > 0` is required for redpanda/approximated sources.

- [ ] **Step 1: Write failing tests**
  - Render golden files for postgres→snowflake and http_server(approximated)→drop, under `plan/testdata/`.
  - `TestRenderRejectsUnrunnable`.
  - Runner `TestLoadE2EScenarioGolden` loads both goldens through `LoadScenario` without error. Plus validation-error rows for e2e without a sink, and `stack: none` on a source direction.
  - CLI exit codes.
- [ ] **Step 2: Run** `go test ./plan/ ./cmd/benchplan/ ./runner/ -run 'Render|E2EScenario|Benchplan' -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit** `bench(plan): render plans to e2e scenarios; benchplan CLI`.

### Task 3: `e2eTopology` — measure at the output

**Files:**
- Create: `benchmarking/aws/runner/topology_e2e.go`
- Modify: `benchmarking/aws/runner/topology.go` (`topologyFor` gains `DirectionE2E = "e2e"`), `benchmarking/aws/runner/matrix.go` (summary source)
- Test: `benchmarking/aws/runner/topology_e2e_test.go`, `benchmarking/aws/runner/matrix_test.go`

**Interfaces:**
- Consumes: `Scenario.Sink`, `Scenario.Pipeline`.
- Produces:
  - `Pipeline(s, n)` returns the scenario input unchanged. The output is the scenario output with `{"benchmark":{"interval":"1s","count_bytes":true}}` **appended last** in `output.processors`, creating the list if absent. The customer's own processors stay under `pipeline.processors`.
  - The `Topology` interface gains `SummaryFromSamples() bool`. It is true for e2e and false for source.
  - When true, `MatrixRunner` computes `PointResult.Summary` with `Summarise(samples)` (the benchmark processor's rolling stats in the Connect log), not from broker series. The empty-broker-series guard is skipped. `BrokerSeries` stays empty.
  - `MetricSidecar` returns empty for e2e. `EngineSeries` returns `nil, nil`.

- [ ] **Step 1: Write failing tests**
  - `TestE2EPipelineAppendsBenchmarkLast`: the output already has `processors: [{mapping: x}]`, and the result is `[mapping, benchmark]` with benchmark last (Review Focus 3).
  - `TestE2EPipelineCreatesProcessorsList`.
  - `TestMatrixSummaryFromSamplesForE2E`: uses the existing fake SSM / log fetcher in `matrix_test.go`. An e2e point with rolling-stats lines and no broker metrics gets a non-zero `Summary.MedianMBPerSec` equal to `Summarise(samples)`.
  - `TestSourceTopologyUnchanged`: existing source tests still pass.
- [ ] **Step 2: Run** `go test ./runner/ -run 'E2E|SummaryFromSamples' -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./runner/...`. Expect PASS.
- [ ] **Step 5: Commit** `bench(runner): e2e topology measures throughput at the output`.

### Task 4: Redpanda and approximated sources (preload)

**Files:**
- Create: `benchmarking/aws/runner/preload.go`
- Modify: `benchmarking/aws/runner/main.go` (call the preload instead of seed/workload for redpanda/approximated sources; `stack: none` skips the stack terraform), `benchmarking/aws/runner/scripts.go` (reset for redpanda sources)
- Test: `benchmarking/aws/runner/preload_test.go`

**Interfaces:**
- Produces:
  - `func renderPreloadScript(topic string, brokers string, messages int64, sizeBytes int) string`. It is a load-gen host script that runs Connect with `generate` (count `messages`, a mapping producing a JSON object padded to `sizeBytes`) into `redpanda` topic `topic` with `max_in_flight: 64`, then checks the high watermark ≥ `messages` with `rpk topic describe -p`. It uses the `redpanda_broker_endpoints` output, never localhost (see the 2026-09-22 abandoned soak spec).
  - Preload sizing: `preload_messages = ceil(expected_peak_mb_s_at_8vcpu × 1e6 × (warmup+duration) × 1.5 / sizeBytes)`. `expected_peak_mb_s` defaults to 300 when the dataset doesn't set it.
  - Reset between points: the input's consumer group becomes `bench_<session>_<point>`, set through the `${BENCH_CONSUMER_GROUP}` placeholder, so each point reads from the start.
  - Placeholders substituted at render: `${BENCH_REDPANDA_BROKERS}`, `${BENCH_TOPIC}` = `bench_<session>_src`, `${BENCH_CONSUMER_GROUP}`.

- [ ] **Step 1: Write failing tests**
  - Preload script contains the broker endpoints from outs, the generate count, the size and the watermark check.
  - Sizing formula: 300 MB/s, 17 min, 1200 B gives a pinned count.
  - Per-point consumer groups differ.
  - `stack: none` skips the stack terraform. Use the existing terraform fake seam, or add an injectable func if none exists, and say so in the report.
- [ ] **Step 2: Run** `go test ./runner/ -run 'Preload|ConsumerGroup|StackNone' -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./runner/...`. Expect PASS.
- [ ] **Step 5: Commit** `bench(runner): redpanda and approximated sources via topic preload`.

### Task 5: Sink framework, plus redpanda, s3 and drop sinks

**Files:**
- Create: `benchmarking/aws/runner/sink.go`, `benchmarking/aws/runner/sink_basic.go`
- Modify: `benchmarking/aws/runner/main.go` (provision before the sweep, teardown deferred, count per point)
- Test: `benchmarking/aws/runner/sink_test.go`

**Interfaces:**
- Produces:
  ```go
  type Sink interface {
      Provision(ctx context.Context, session string) (placeholders map[string]string, err error) // values for ${BENCH_*} output placeholders
      Count(ctx context.Context, point string) (int64, error)  // records written for this point; ErrCountUnsupported if none
      Teardown(ctx context.Context, session string) error       // idempotent
  }
  func sinkFor(spec SinkSpec, deps SinkDeps) (Sink, error)
  ```
- The three sinks:
  - `redpanda`: topic `bench_<session>_dst`. Count is the high-watermark delta per point, from the broker endpoints.
  - `s3`: bucket = the session results bucket, prefix `sink/<session>/<point>/`. Count is the number of objects multiplied by the batch count, so it returns `ErrCountUnsupported` unless `batching.count` is fixed. Teardown is a no-op, because the bucket is `force_destroy`.
  - `drop`: Count returns `ErrCountUnsupported`.
- **Ground truth:** when Count is supported, compare it with Σ(msg/sec samples) for the point. If they disagree by more than 2%, mark the point failed with `anomaly: sink_count_mismatch`, reusing the existing `Anomaly` type.

- [ ] **Step 1: Write failing tests**
  - `sinkFor` returns the right kind and errors on an unknown kind.
  - Redpanda placeholders and the count delta, against a fake broker client.
  - The 2% rule: 1.5% passes and 2.5% gives an anomaly.
  - Teardown runs even when the sweep errors, using a fake sweep that returns an error.
- [ ] **Step 2: Run** `go test ./runner/ -run 'Sink' -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./runner/...`. Expect PASS.
- [ ] **Step 5: Commit** `bench(runner): sink framework with redpanda, s3 and drop sinks`.

### Task 6: Iceberg sink (S3 + Glue Iceberg REST catalog)

**Files:**
- Create: `benchmarking/aws/runner/sink_iceberg.go`
- Modify: `benchmarking/aws/terraform/shared/` (an S3 warehouse bucket with `force_destroy` and an IAM grant for Glue on the runner role), `benchmarking/aws/cleanup-lambda/sweep.go` (Glue databases named `bench_*` older than 4h)
- Test: `benchmarking/aws/runner/sink_iceberg_test.go`, `benchmarking/aws/cleanup-lambda/sweep_test.go`

**Interfaces:**
- Provision creates the Glue database `bench_<session>`. Placeholders:
  - `${BENCH_ICEBERG_CATALOG_URL}` = `https://glue.<region>.amazonaws.com/iceberg`
  - `${BENCH_ICEBERG_WAREHOUSE}` = `<account_id>`
  - `${BENCH_ICEBERG_NAMESPACE}` = `bench_<session>`
  - The output's catalog auth is set to `aws_sigv4` (region, service `glue`) by the identity swap template from Task 1. Update that template here if Task 1 left it generic.
- Count: `ErrCountUnsupported` for now.
- Teardown deletes the Glue database and its tables, and empties the warehouse prefix.
- Uses a `GlueAPI` interface (CreateDatabase, DeleteDatabase, GetTables, DeleteTable), with a fake in tests.
- Reaper: selects Glue databases with prefix `bench_` and CreateTime older than 4h (Review Focus 4).

- [ ] **Step 1: Write failing tests:** provision/teardown call sequences on the fake, teardown idempotent, reaper selection by age and prefix.
- [ ] **Step 2: Run** `go test ./runner/ -run Iceberg -v` and `cd cleanup-lambda && go test ./... -run Glue -v`. Expect FAIL.
- [ ] **Step 3: Implement**, plus Terraform. Run `terraform fmt -check`, and `terraform validate` with `-backend=false` in `terraform/shared`.
- [ ] **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit** `bench(runner): iceberg sink on S3 with Glue REST catalog`.

### Task 7: Snowflake sink

**Files:**
- Create: `benchmarking/aws/runner/sink_snowflake.go`
- Modify: `benchmarking/aws/cleanup-lambda/sweep.go` (Snowflake schemas `BENCH_*` older than 4h)
- Test: `benchmarking/aws/runner/sink_snowflake_test.go`, `benchmarking/aws/cleanup-lambda/sweep_test.go`

**Interfaces:**
- Credentials come from the Secrets Manager secret `redpanda-connect-bench/snowflake`, as JSON `{account, user, private_key, database, warehouse, role}`. They are fetched like the licence (`runner/secrets.go` pattern). Never log them.
- Provision:
  - `CREATE SCHEMA BENCH_<SESSION>` in the bench database.
  - `ALTER WAREHOUSE <wh> SET WAREHOUSE_SIZE = <spec.Size or XSMALL>`.
  - The customer's target table is created by the output's schema-evolution setting. If the output has none, `CREATE TABLE BENCH_<SESSION>.T (RECORD VARIANT)` and the identity template points the output at it.
  - Placeholders: `${BENCH_SNOWFLAKE_ACCOUNT}`, `_USER`, `_PRIVATE_KEY` (passed by env reference, not inlined into the config file), `_DATABASE`, `_SCHEMA`, `_WAREHOUSE`, `_ROLE`.
- Count: `SELECT COUNT(*)` delta per point.
- Teardown: `DROP SCHEMA IF EXISTS … CASCADE`, then reset the warehouse size to `XSMALL`.
- `SnowflakeExec` interface (Exec, QueryInt) with a fake. Use a real driver adapter on `gosnowflake`.
- Reaper: lists schemas `BENCH_%` with `created < now-4h`.

- [ ] **Step 1: Write failing tests:**
  - SQL issued on provision, count and teardown, against the fake.
  - Size defaults to XSMALL.
  - The private key never appears in placeholders' values: the value is an env reference. Assert on the rendered config.
  - Reaper selection.
- [ ] **Step 2: Run.** Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit** `bench(runner): snowflake sink with per-run schema and reference warehouse`.

### Task 8: BigQuery sink

**Files:**
- Create: `benchmarking/aws/runner/sink_bigquery.go`
- Modify: `benchmarking/aws/cleanup-lambda/sweep.go` (datasets `bench_*` older than 4h)
- Test: `benchmarking/aws/runner/sink_bigquery_test.go`, `benchmarking/aws/cleanup-lambda/sweep_test.go`

**Interfaces:**
- Credentials come from Secrets Manager `redpanda-connect-bench/bigquery`, as JSON `{project, credentials_json}`. The credentials are written to a 0600 file on the runner host and referenced by path.
- Provision: dataset `bench_<session>`, plus table `t` with a single `JSON` column when the output doesn't define a schema. Placeholders: `${BENCH_BQ_PROJECT}`, `${BENCH_BQ_DATASET}`, `${BENCH_BQ_TABLE}`, `${BENCH_BQ_CREDENTIALS_FILE}`.
- Count: `SELECT COUNT(*)` delta per point.
- Teardown: delete the dataset with its contents.
- `BigQueryAPI` interface with a fake. Reaper: datasets `bench_` with creation time older than 4h.

- [ ] **Steps 1–5** as in Task 7. Commit `bench(runner): bigquery sink with per-run dataset`.

### Task 9: Catalogue additions — unreleased builds, plan caveats, estimates

**Files:**
- Modify: `benchmarking/aws/catalog/entry.go`, `catalog/fromrun.go`, `catalog/staleness.go`, `catalog/sizing.go` (caveats)
- Create: `benchmarking/aws/catalog/estimates.go`, `benchmarking/aws/catalog/estimates.json`
- Modify: `benchmarking/aws/runner/catalogentry.go` (pass the e2e pipeline and caveats)
- Test: matching `_test.go` files

**Interfaces:**
- `func (e Entry) Unreleased() bool`: true when `ConnectVersion` matches `-\d+-g[0-9a-f]+$`. `Recommend` adds caveat `unreleased_build`.
- Entry gains `SourceApproximated bool \`json:"source_approximated"\`` and `Caveats []string \`json:"caveats,omitempty"\`` (from the plan). RunInput gains the same, and `EntryFromRun` copies them. `Recommend` merges `e.Caveats` into its caveats, de-duplicated and in a stable order.
- For e2e entries, RunInput.Pipeline is the scenario's pipeline: the identity-swapped customer pipeline with no harness benchmark processor, so `Fingerprint` equals the fingerprint of the customer's original config. That holds because every swapped key is either in `fingerprint.go`'s ignored keys, or a blanked string present on both sides. **Test this equality explicitly.** If it fails, add the missing keys to fingerprint's ignored list and say so.
- Staleness: for an unreleased entry, `toRef` = `origin/main`, or `main` if there's no remote, instead of the latest tag.
- `estimates.json`: `{"stacks": {"postgres": {"usd": 2.5, "minutes": 25}, "mysql": {...}, "none": {...}}, "sinks": {"snowflake": {"usd_per_xsmall_sweep": 1.0, "size_multiplier": {"XSMALL":1,"SMALL":2,"MEDIUM":4,"LARGE":8}}, "bigquery": {"usd": 0.5}, "iceberg": {"usd": 0.2}, "s3": {"usd": 0.05}, "redpanda": {"usd": 0}, "drop": {"usd": 0}}}`.
  - `type Estimate struct { AWSUSD, SaaSUSD float64; Minutes int }`
  - `func EstimateRun(stack, sink, sinkSize string) (Estimate, error)`
  - These are starting values; the Connect PM owns the file.
- Plan (Task 1) now fills `Estimate` from `EstimateRun`. Update Task 1's Build and tests here.

- [ ] **Step 1: Write failing tests:**
  - `Unreleased` true/false table.
  - The `unreleased_build` caveat appears in `Recommend`.
  - Plan caveats flow through `EntryFromRun` into the recommendation.
  - Customer-config fingerprint equals the e2e entry fingerprint, for the postgres→snowflake fixture from Task 1.
  - Staleness `toRef` for unreleased entries.
  - `EstimateRun` for snowflake MEDIUM gives SaaS 4.0.
- [ ] **Step 2: Run** `go test ./catalog/... ./plan/... ./runner/... -v -run 'Unreleased|PlanCaveat|E2EFingerprint|EstimateRun|UnreleasedStaleness'`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the full `go test ./catalog/... ./plan/... ./runner/... ./cmd/...`. Expect PASS.
- [ ] **Step 5: Commit** `bench(catalog): unreleased builds, plan caveats and run estimates`.

### Task 10: Plan store and stack lock (S3)

**Files:**
- Create: `benchmarking/aws/plan/store.go`, `benchmarking/aws/plan/lock.go`
- Test: `benchmarking/aws/plan/store_test.go`, `benchmarking/aws/plan/lock_test.go`

**Interfaces:**
- `type Store interface { Put(ctx, Plan) error; Get(ctx, id string) (Plan, error); MarkDispatched(ctx, id string, runID int64) (Plan, bool, error) }`
  - Key: `plans/<id>.json` in `redpanda-connect-bench-catalog`.
  - `MarkDispatched` uses a conditional put on `plans/<id>.dispatched` (If-None-Match `*`). The bool is true only for the first caller.
  - `NewS3Store(api, bucket)` and `NewDirStore(root)`. S3API gains `PutObject` with `IfNoneMatch`.
- `type Lock interface { Acquire(ctx, holder string, ttl time.Duration) (bool, error); Release(ctx, holder string) error }`
  - Object `locks/shared-stack` holds JSON `{holder, expires_at}`, written with If-None-Match.
  - An expired lock is replaced with If-Match on its ETag.
  - Release deletes only when `holder` matches.
  - `cmd/benchplan` gains `lock acquire --holder H --ttl 2h --wait 4h --poll 2m` and `lock release --holder H`. `acquire` also waits until no `Project=redpanda-connect-bench` EC2 instance is running, via an `EC2API` interface.

- [ ] **Step 1: Write failing tests** (Review Focus 5):
  - Two acquirers, only one wins.
  - An expired lock is taken over.
  - Release by a non-holder is a no-op.
  - `MarkDispatched` is true once, then false.
  - `acquire` waits while a fake EC2 instance reports running.
- [ ] **Step 2: Run** `go test ./plan/ ./cmd/benchplan/ -run 'Lock|Store|Dispatched' -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit** `bench(plan): S3 plan store, idempotent dispatch marker and shared-stack lock`.

### Task 11: `bench_bot.yml` workflow and watchdog

**Files:**
- Create: `.github/workflows/bench_bot.yml`, `.github/workflows/bench_bot_watchdog.yml`
- Modify: `.github/workflows/actions.lock` (run `gh actions-lock`, per CLAUDE.md)
- Test: `benchmarking/aws/runner/workflow_test.go` (parses both YAMLs and asserts the invariants below)

**Interfaces:**
- `bench_bot.yml`:
  - Triggers: `on: workflow_dispatch: inputs: plan_id (required, string)`. `run-name: bench-bot ${{ inputs.plan_id }}`. No `concurrency:` key. `timeout-minutes: 330`.
  - Permissions: `id-token: write`, `contents: read`.
  - Steps:
    1. Validate `plan_id` against `^[0-9a-f]{12}$`.
    2. Checkout with `fetch-depth: 0`.
    3. AWS OIDC, then the licence, as in `soak_nightly.yml`.
    4. `benchplan show` and `benchplan render --out $RUNNER_TEMP`.
    5. `benchplan lock acquire --holder ${{ github.run_id }} --ttl 2h --wait 4h`.
    6. `task aws:bench` with the rendered scenario path.
    7. `if: always()`: `benchplan lock release`.
    8. `if: always()`: the completion A2A message, below.
  - **Completion step.** It POSTs a JSON-RPC `message/send` with `configuration.blocking: false` to `${{ vars.BENCH_BOT_A2A_URL }}`. Auth is an oauth2 client-credentials token using `secrets.BENCH_BOT_ADP_CLIENT_ID`/`_SECRET`, token URL `https://auth.prd.cloud.redpanda.com/oauth/token`, audience `cloudv2-production.redpanda.cloud`.
    - The message body is the text `BENCH RESULT plan=<id> run=<url> outcome=<success|failure|cancelled> entry=<entry id or none>`.
    - If the vars or secrets are missing, it prints a warning and exits 0.
- `bench_bot_watchdog.yml`: runs `schedule: '*/30 * * * *'`. It lists `plans/*.dispatched` older than 6h without a `plans/<id>.done` marker. For each, it sends `BENCH RESULT plan=<id> run=<url> outcome=never_reported entry=none` and writes `.done`. The completion step also writes `.done`, via a new subcommand `benchplan done --plan <id>`; add it with a test.
- Workflow test asserts:
  - no `concurrency` key in bench_bot.yml;
  - `plan_id` validation runs before any use;
  - both the lock release and the completion step have `if: always()`;
  - every `uses:` is pinned to a SHA.

- [ ] **Step 1: Write failing tests:** the workflow assertions above, plus the `benchplan done` subcommand.
- [ ] **Step 2: Run.** Expect FAIL.
- [ ] **Step 3: Implement**, then run `gh actions-lock`.
- [ ] **Step 4: Run** `go test ./runner/ -run Workflow -v` and `go test ./cmd/benchplan/...`. Expect PASS.
- [ ] **Step 5: Commit** `bench: bench_bot workflow, completion message and watchdog`.

### Task 12: Operator setup and first end-to-end run (needs AWS, Snowflake and BigQuery access)

**Files:** none (operational). The controller does not run this. It is handed to the user.

- [ ] **Step 1: Store sink credentials.** `aws secretsmanager create-secret` for `redpanda-connect-bench/snowflake` (key-pair auth for a bench user with rights to create schemas in a bench database and resize one warehouse) and `redpanda-connect-bench/bigquery` (a service account with BigQuery Data Editor and Job User on a bench project).
- [ ] **Step 2: Apply infrastructure.** `task aws:persistent` (catalogue bucket, reaper) and the shared-stack Terraform changes. Set the repo var `BENCH_BOT_A2A_URL` and the secrets `BENCH_BOT_ADP_CLIENT_ID`/`_SECRET` once part 2b's agent exists. Until then, completion messages are skipped with a warning.
- [ ] **Step 3: Run a test plan.** Write a postgres→snowflake config with `benchplan build --config cfg.yaml --message-size 1024 --target-mb-s 40 --store s3://redpanda-connect-bench-catalog`. Add `build` to the Task 2 CLI if it's missing; it calls `plan.Build` and `Store.Put`. Then `gh workflow run bench_bot.yml -f plan_id=<id>`.
- [ ] **Step 4: Verify** that the run's catalogue entry exists, that `catalog recommend` on it returns CUs with `unreleased_build` where expected, and that no `BENCH_*` schema or dataset remains in Snowflake or BigQuery.
