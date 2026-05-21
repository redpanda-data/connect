# Bucket 2 — bench foundation polish

Date: 2026-05-21
Author: Prakhar (with Claude)
Status: draft — awaiting review
Predecessor: [2026-05-19-aws-benchmarking-framework-design.md](./2026-05-19-aws-benchmarking-framework-design.md)
Successor (when written): the bucket-2 implementation plan

## Why this exists

Bucket 1 verified the AWS bench framework end-to-end on real infra and published the first throughput row for `postgres / orders-cdc`. Before adding more connectors (mysql first, then the rest of bucket 3), four foundation gaps need closing so that future bench rounds are unattended-safe, cost-observable, anomaly-diagnosable, and self-summarising.

This spec covers all four items in one design and one implementation plan. Each item is independent at the file/PR level and can be shipped separately.

## Goals

- A bench run that dies for any reason — SIGKILL, expired creds, crashed orchestrator, network drop — leaves no resources behind for more than ~3 hours.
- The operator can answer "what has this project cost me this week?" in one command.
- Every sweep point has a parsed time-series of Connect's process-level metrics so anomalies (throughput dips, zero-sample windows) are diagnosable after the fact.
- `docs/benchmark-results/SUMMARY.md` shows the latest AWS bench numbers per connector/scenario without any hand-editing.

## Non-goals

- No retroactive Prometheus capture for past runs. Only future bench runs gain the snapshot stream.
- No per-session cost attribution from the bench output. Cost Explorer lags 24–48 hours; pretending to attribute live spend would mislead.
- No heartbeat-based orphan model. TTL is sufficient for the bench's actual usage pattern (≤90-minute runs); a heartbeat scheme can be added later if a real long-running scenario emerges.
- No backfill of the existing hand-written rows in `SUMMARY.md`. Those come from `internal/impl/*/bench/` (laptop Docker) and stay manually curated.

## Cross-cutting

- All new Go files carry the existing BSL license header used elsewhere in `benchmarking/aws/runner/`.
- Every new AWS-touching interface follows the existing `FakeSSM` / `FakeLogFetcher` pattern so unit tests stay AWS-free.
- New external dependencies: none. The Prom parser is hand-rolled over the curated subset; Cost Explorer and EventBridge are already in the project's `aws-sdk-go-v2` tree.

## Item 1: Orphan-cleanup Lambda

### Detection model

TTL-based. A tagged resource is an orphan when its creation time (`LaunchTime` for EC2, `InstanceCreateTime` for RDS, `CreationDate` for S3, etc.) is older than `orphanTTL = 3 * time.Hour`. The constant lives in the Lambda code; an env var `BENCH_ORPHAN_TTL_HOURS` (decimal hours, e.g. `2.5`) on the Lambda function overrides it for one-off long benches.

The Lambda uses two existing tags as its contract:
- `Project=redpanda-connect-bench` — "is this ours"
- `bench-session-id=bench-<UTC-timestamp>` — informational, surfaced in the SNS message; not used for filtering

### Trigger

EventBridge rule `rate(15 minutes)`. ~3K invocations/month, well inside the free tier. No manual invoke path beyond what the AWS Console offers natively.

### Action

Auto-destroy via direct AWS API calls. No `terraform` binary in the Lambda image; no state-file access; no DDB-lock contention with a live bench. Best-effort: log and continue on per-resource failure, aggregate errors at the end.

### Sweep order

Strict, to satisfy AWS delete-time dependencies:

1. RDS DB Instances (cost-bearing, slowest delete — kick off first)
2. EC2 Instances
3. NAT Gateways (none today, future-proof)
4. S3 Buckets (empty before delete: list → delete objects → delete bucket)
5. IAM Role Policies, Instance Profiles, Roles
6. Security Groups (wait for ENI release from terminated instances)
7. Route Table Associations, Route Tables
8. Internet Gateways (detach from VPC first)
9. Subnets
10. VPCs
11. DB Subnet Groups, DB Parameter Groups

### Discovery

`resourcegroupstaggingapi:GetResources` with the `Project` tag filter returns ARNs grouped by service. The Lambda parses ARNs, fetches per-resource creation time via service-specific Describe calls, applies TTL, dispatches to the right delete API.

### Notification

SNS topic `redpanda-connect-bench-orphans` (provisioned in shared TF). The Lambda publishes **only when something was destroyed** — no noise on the every-15-min no-op runs. Email subscription is operator-managed in the Console; the Lambda doesn't require it. CloudWatch Logs always capture the full sweep regardless.

### Package layout

```
benchmarking/aws/cleanup-lambda/
  main.go         — Lambda handler entrypoint
  sweep.go        — TTL filter + dispatch table per resource type
  sweep_test.go   — table-driven tests with FakeAWS mock
  Makefile        — `make zip` builds a linux/arm64 bootstrap zip
```

Terraform additions live in `benchmarking/aws/terraform/shared/cleanup.tf`:

- `aws_lambda_function` — runtime `provided.al2023`, arch `arm64`, handler `bootstrap`, package from the `make zip` artifact
- IAM role + policy with the union of Describe + Terminate/Delete permissions across all resource types listed above, plus `sns:Publish` and `resourcegroupstaggingapi:GetResources`
- `aws_cloudwatch_event_rule` (rate 15 min) + `aws_cloudwatch_event_target` → Lambda
- `aws_lambda_permission` for EventBridge → Lambda invoke
- `aws_sns_topic` `redpanda-connect-bench-orphans`

### Testing

Table-driven `sweep_test.go` using a `FakeAWS` struct that satisfies a narrow `cleanupAPI` interface. Three cases:
- All resources younger than TTL → nothing destroyed, no SNS publish
- Mix of fresh + stale → only stale destroyed, in dependency order, one SNS publish
- Per-resource API failure → other resources still attempted, error aggregated, SNS publish includes the failure list

No live-AWS tests for the Lambda — the same APIs are already exercised by every live bench run.

## Item 2: cost-check subcommand

### CLI

```
runner cost-check [--days=7] [--region=us-east-2]
```

Wired into the Taskfile as `task aws:cost-check`. Defaults match the canonical case; flags exist for completeness.

### Output

```
AWS spend — Project=redpanda-connect-bench (us-east-2)
Note: AWS Cost Explorer lags ~24-48h; today's spend will be partial.

  today         $0.00
  last 7 days   $18.42
  month-to-date $42.91

By usage type (last 7 days):
  EC2 (c8g.4xlarge)     $9.20
  RDS (db.r6g.2xlarge)  $7.10
  EBS (gp3 + snapshots) $1.45
  Data Transfer         $0.42
  Other                 $0.25
```

Plain text. No JSON output, no fancy formatting beyond column alignment. Numbers come from Cost Explorer with no roll-your-own attribution math.

### API calls

Two `GetCostAndUsage` calls against `costexplorer:us-east-1` (CE is global, exposed only in us-east-1):

1. **Totals.** `TimePeriod` covering the current month, `Granularity=DAILY`, `Metrics=["UnblendedCost"]`, filter `Tags: Project=redpanda-connect-bench`. Collapse client-side to today / last-7-days / month-to-date.
2. **Breakdown.** Same filter, `GroupBy=[USAGE_TYPE]`, last 7 days.

### Package layout

```
benchmarking/aws/runner/
  cost.go        — CostExplorer interface, awsCostExplorer impl, FakeCostExplorer, Summarise/Print
  cost_test.go   — table-driven with FakeCostExplorer + canned CE responses
```

### Error modes

- **Cost Explorer not enabled** → message: `"Cost Explorer is not enabled for account <id>; enable in Billing console then retry."` and exit 1.
- **Permission denied** → fail with the underlying SDK error wrapped. The `AdministratorAccess` SSO profile already grants `ce:GetCostAndUsage`.
- **No data yet (fresh project)** → print zeros, exit 0.

## Item 3: Prometheus snapshot capture

### On-runner scrape

Connect already serves Prometheus metrics on `localhost:4195/metrics` (from `http.debug_endpoints: true` + `metrics: prometheus: …` in `renderPipelineConfig`). The bench script gains a third background subshell — alongside the existing benchmark loop and heartbeat — that scrapes every 10 s for the lifetime of the Connect process and appends framed snapshots to `/tmp/prom-N.txt`:

```bash
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%s)"
      curl -s --max-time 5 http://localhost:4195/metrics || echo "###scrape_error"
    } >> /tmp/prom-N.txt
    sleep 10
  done
) &
PROM_SCRAPER=$!
# ... existing sleep / kill / wait ...
kill "$PROM_SCRAPER" 2>/dev/null || true
aws s3 cp /tmp/prom-N.txt "s3://$BUCKET/runs/$SESSION_ID/prom-$VCPU.txt" >/dev/null
```

Snapshot framing is plain Prometheus text exposition format wrapped between `###timestamp=<epoch>` and (where applicable) `###scrape_error` markers. ~50 KB per snapshot × ~100 snapshots = ~5 MB per sweep point. Uploaded under the same `runs/<session>/` prefix as the per-point Connect log.

### Orchestrator fetch + parse

`MatrixRunner.fetchLog` gains a sibling `fetchProm(ctx, vcpu)` using the same `LogFetcher` interface with a new key `runs/<session>/prom-<vcpu>.txt`. The parser walks `###timestamp=` framing and extracts a **curated subset** into a slice of `PromPoint`:

| Metric | Why |
|---|---|
| `go_goroutines` | Detect leak / unbounded fan-out |
| `go_memstats_heap_inuse_bytes` | Memory pressure |
| `go_memstats_gc_pause_total_ns` | GC stalls (per-snapshot derivative) |
| `process_cpu_seconds_total` | Confirm Connect is actually using its allotted vCPUs |
| `benchmark_bytes_total` | Cross-check against rolling-stats parsed throughput |

### Parser implementation

Hand-rolled, `strings.SplitN`-based. The curated subset is five metric names with no labels and no histogram/summary handling required. Avoids pulling `github.com/prometheus/common/expfmt` and its transitive deps. ~50 LoC.

### Result JSON shape

`PointResult` gains a `Prom []PromPoint` field, `omitempty` so historical fixture tests stay green. `PromPoint` is `{T int, Goroutines int, HeapInUseMB float64, BytesTotal float64, CPUSeconds float64, GCPauseTotalNS uint64}`. `T` is seconds since end-of-warmup, matching the existing `Sample.T` convention.

### Anomaly correlation

`Anomaly` gains optional fields populated from the prom snapshot nearest to the anomaly's start time: `GoroutinesAtStart`, `HeapInUseMBAtStart`, `GCPauseDeltaNS`. The auto-rendered markdown anomaly section will become diagnosable at a glance ("dip at T=240s; goroutines 312 → 1841, heap 100MB → 1.4GB"). Backwards-compatible: when `Prom` is empty (older runs, tests), these fields stay zero.

### Package layout

```
benchmarking/aws/runner/
  prom.go        — PromPoint, parser, curated extractor
  prom_test.go   — canned /metrics fixture parse + corrupted-snapshot survival
  testdata/
    prom-sample.txt — captured from a real Connect run
  matrix.go      — extended (scrape sidecar, fetchProm wired in)
  anomalies.go   — extended (prom context fields on Anomaly)
  render.go      — extended (PointResult.Prom)
```

### Risks

- Scraper subshell can race with Connect shutdown; the `kill -0 $PID` loop exits cleanly on SIGTERM. Worst-case the final snapshot is partial; the parser skips snapshots that don't end with a newline.
- `localhost:4195/metrics` slow → `--max-time 5` curl bound. Snapshot logged as `###scrape_error`; parser logs and skips.
- Free disk on the runner — 5 MB per point × 4 points = 20 MB. Negligible vs c8g.4xlarge's 30 GB root volume.

## Item 4: SUMMARY.md auto-refresh

### Marker contract

Two HTML comments inserted into `docs/benchmark-results/SUMMARY.md` exactly once, in the same PR that ships this feature:

```markdown
<!-- bench:aws:start - auto-generated, do not edit by hand -->
<!-- bench:aws:end -->
```

The renderer replaces only the bytes between these two markers. Everything outside is preserved byte-exact. If markers are missing (someone deleted them, fresh checkout), the renderer appends the section to the end of the file and writes a warning to stderr.

### Trigger

Called from the end of `runBench`, after `AppendMarkdown`, as `RefreshSummary(summaryPath, resultsDir)`. Also available as a standalone subcommand `runner summary` (Taskfile: `task aws:summary`) so the section can be regenerated without paying for a real bench — useful after a manual edit to the file or for CI verification.

### Data source

Walks `benchmarking/aws/results/<connector>/<scenario>/*.json`. For each `(connector, scenario)` pair, picks the JSON with the latest timestamp suffix. For each picked result, derives:

- **Peak MB/s** — `max(point.summary.peak_mb_s)` across sweep points where peak > 0
- **Best vCPU** — the point that produced that peak
- **Median MB/s at best vCPU** — `summary.median_mb_s` of that point
- **Last run date** — `finished_at` truncated to YYYY-MM-DD

When every sweep point has `peak_mb_s == 0` (catastrophic failure), the row renders with em-dashes and a footnote pointing at the result JSON.

### Rendered region (example)

```markdown
<!-- bench:aws:start - auto-generated, do not edit by hand -->
## AWS Bench Results

Last refreshed: 2026-05-21

| Connector / Scenario  | Peak MB/s | At vCPU | Median (best vCPU) | Last Run    |
|-----------------------|-----------|---------|--------------------|-------------|
| postgres / orders-cdc |       102 |       4 |                 99 | 2026-05-21  |

Each row is the **latest** run of that scenario. Raw samples + Prometheus snapshots live under `results/<connector>/<scenario>/`.

To regenerate without running a bench: `runner summary --repo-root=.`
<!-- bench:aws:end -->
```

### Write strategy

Write to `SUMMARY.md.tmp`, then `os.Rename` to `SUMMARY.md`. No partial-file risk if the runner gets SIGINTed mid-write. Matches the existing pattern in `WriteResultJSON`.

### Package layout

```
benchmarking/aws/runner/
  summary.go         — RefreshSummary, walkResults, marker-bounded write
  summary_test.go    — t.TempDir() fixtures
  templates/
    summary-section.md.tmpl — embedded via go:embed
  main.go            — extended (`summary` subcommand dispatch)
```

### Testing

Table-driven `summary_test.go`:
- Empty results dir → empty-state placeholder, no error
- One scenario, two timestamped JSONs → only the newest used
- Two scenarios → two rows, sorted alphabetically by `connector/scenario`
- Existing handwritten content above/below markers → preserved byte-exact
- Missing markers → append + warning to a captured stderr
- Latest JSON has all zero peaks → row renders with em-dashes + footnote

### Race

`AppendMarkdown` and `RefreshSummary` write to different files. `runBench` is single-threaded. Call sequentially at the end of `runBench`.

## Implementation order (suggested)

These can land in any order. If sequencing matters for review load:

1. **cost-check** first — smallest, no infra changes, valuable immediately, exercises Cost Explorer auth path that the Lambda will reuse.
2. **SUMMARY.md auto-refresh** — pure code, no infra. Quick win, makes the project surface look maintained.
3. **Prom snapshot capture** — touches bench-script + matrix + render. Bigger code change but no infra.
4. **Orphan-cleanup Lambda** — biggest piece (Lambda code + terraform + IAM). Has the largest blast radius (it deletes things), needs the most careful review.

## What "done" looks like

- `task aws:cost-check` prints a real CE summary.
- `task aws:bench` completes a sweep, the result JSON contains a populated `prom` array per point, and the auto-appended postgres.md table includes per-anomaly goroutine/heap context.
- `docs/benchmark-results/SUMMARY.md` has the auto-generated section between markers; running `task aws:summary` after a no-op edit restores it.
- The Lambda is deployed in the shared stack; deliberately leaving a manually-tagged test resource older than 3 hours triggers a destroy on the next 15-minute schedule and an SNS notification.

## Out of scope (deferred to bucket 3 or later)

- mysql / sqlserver / dynamodb / s3 / iceberg / kafka-to-pg scenarios (bucket 3).
- Per-point table truncation in scenarios to avoid the cumulative-growth issue we hit at 8 vCPU. Scenario-design concern, addressed when writing each new scenario. Lessons captured in the `bench-scenario-sizing-lessons` memory.
- Multi-region support (currently us-east-2 only).
- Cost budget alerts (could layer on Item 2 later if needed).
- Snapshotting node-level (OS) metrics from the runner instance. The Connect-process metrics are sufficient for the anomalies seen so far; node-level can layer in later via `node_exporter` if needed.
