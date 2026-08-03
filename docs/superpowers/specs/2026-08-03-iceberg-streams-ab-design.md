# Iceberg sink: 1 pipeline vs 2 streams-mode pipelines at 2 vCPU

**Date:** 2026-08-03
**Status:** approved, pending implementation
**Scope:** `benchmarking/aws/` — new `matrix.arms` sweep dimension (sink-only) plus one
new scenario that uses it.

## Question

At a 2-core allocation, does splitting one `iceberg` output pipeline into two
streams-mode pipelines buy throughput when `GOMAXPROCS=4` gives the Go runtime
headroom beyond the pinned cores?

Two facts motivate it:

- Connect counts cores for licensing off the machine's CPU count, not
  `GOMAXPROCS`, so oversubscribing `GOMAXPROCS` past the core allocation is free
  in licensing terms.
- The `iceberg` output is commit-latency-bound (Glue REST + S3 round-trips), so
  goroutines block on I/O. With `GOMAXPROCS` equal to the core count, blocked
  goroutines can leave the pinned cores idle.

Verified: the tree contains no `automaxprocs` import and no `runtime.GOMAXPROCS`
call in `cmd/` or `internal/cli/`, so the `GOMAXPROCS` environment variable the
bench script sets is authoritative.

## Reference points

From `docs/benchmark-results/iceberg-recipe-comparison.md` (mean MB/s of Iceberg
committed-bytes growth, 2 vCPU): Recipe A **69.1**, Recipe B 64.1, Kafka Connect
64.3. Recipe A is the base for this A/B because it won at 2 vCPU. Those numbers
came from a different session and code SHA, hence the in-session baseline arm
below.

## Arms

Three arms, all at `taskset -c 2-3` (2 measured cores, cores 0-1 reserved), all
Connect-only (`--engines=connect`), all at one `cpu_points` entry of `2`.

| arm ID | launch | GOMAXPROCS | streams | per-stream pipeline |
|---|---|---|---|---|
| `a0-1pipe-gmp2` | `run cfg.yaml` | 2 | 1 | buffer 500 MiB, batch 10000/10s, `max_in_flight: 16` |
| `a1-1pipe-gmp4` | `run cfg.yaml` | 4 | 1 | identical to `a0` |
| `b-2pipe-gmp4` | `streams -o root.yaml streams/` | 4 | 2 | buffer 250 MiB, batch 10000/10s, `max_in_flight: 8` |

`a0` is the in-session baseline. Without it, a win in `b` is unattributable
between `GOMAXPROCS` oversubscription and the pipeline split.

`GOMEMLIMIT` is already derived from vCPU (`matrix.go`, `memLimitPerVCPU * n`),
not from `GOMAXPROCS`, so it is constant across all three arms with no code
change — a fairness property worth preserving deliberately.

Arm B halves each stream's buffer limit and `max_in_flight` so total buffered
memory (500 MiB) and total in-flight budget (16) match arms A. It is therefore a
pure topology comparison, not a resource-budget comparison.

## Wiring

Both arm-B streams consume the same pre-seeded topic under the **same** consumer
group, so the 16 partitions split 8/8 via one group rebalance at startup. Each
stream writes its **own** Glue table:

```
stream_0: topic=..._src, group=..._connect  ->  table ..._connect_s0
stream_1: topic=..._src, group=..._connect  ->  table ..._connect_s1
```

Throughput for the arm is `d(total-files-size[s0] + total-files-size[s1])/dt`.
Separate tables mean no concurrent-commit conflict retries, which keeps the
measurement on the question asked. A single shared table was considered and
rejected as a confound; it is a reasonable follow-up if a customer requires one
table.

## Implementation

Six files, plus a new scenario. The `arms` dimension is deliberately restricted
to `direction: sink` with exactly one engine, which keeps it clear of the Kafka
Connect delta/anomaly machinery.

### 1. `runner/scenario.go`

Add to `MatrixSpec`:

```go
Arms []Arm `yaml:"arms,omitempty"`
```

```go
// Arm is one leg of an A/B at a fixed vCPU point: a launch topology plus
// per-stream pipeline overrides.
type Arm struct {
    ID         string         `yaml:"id"`
    GOMAXPROCS int            `yaml:"gomaxprocs"`
    Streams    int            `yaml:"streams"`
    Pipeline   map[string]any `yaml:"pipeline,omitempty"`
}
```

`Arm.Pipeline` deep-merges over the scenario-level `pipeline` block, so an arm
states only what differs.

Validation, when `len(Arms) > 0`:

- `Direction` must be `sink`.
- `len(CPUPoints)` must be exactly 1 — arms × vCPU would multiply the run and
  make the result table unreadable. Lift later if a need appears.
- IDs unique, non-empty, matching `^[a-z0-9][a-z0-9-]*$` (they land in S3 keys
  and filenames).
- `Streams >= 1`, `GOMAXPROCS >= 1`.

### 2. New `runner/sweepplan.go`

Expands the matrix into an explicit point list:

```go
type sweepPoint struct {
    VCPU       int
    ArmID      string // "" when the scenario declares no arms
    GOMAXPROCS int
    Streams    int
    Pipeline   map[string]any // merged; nil when no arms
}

func (p sweepPoint) Key() string // "2" with no arm; "2-b-2pipe-gmp4" with one
```

**Parity requirement:** a scenario with no `arms` expands to one point per
`cpu_points` entry with `GOMAXPROCS == VCPU`, `Streams == 1`, and
`Key() == strconv.Itoa(VCPU)`. That makes every rendered script and artifact name
byte-identical to today's for all six existing scenarios. This is the primary
regression guard and is unit-tested directly.

### 3. `runner/matrix.go`

- Loop over plan points instead of `cpuPoints`.
- `benchScriptArgs` gains `GOMAXPROCS`, `Streams`, `Key`, `RootConfigPath`.
- The launch line branches:
  - `Streams == 1`: `taskset -c 2-N env GOMAXPROCS=<gmp> ... <bin> run <cfg>`
  - `Streams > 1`: `taskset -c 2-N env GOMAXPROCS=<gmp> ... <bin> streams -o <root> <dir>`
- Artifact paths (`sweep-<key>.log`, `prom-<key>.txt`) key off `Key()`.
- `MetricArtifact(engine, key string)` — signature changes from `vcpu int`.
  `MetricSidecarArgs` correspondingly carries `Key string` alongside `VCPU`,
  since it is what builds the artifact name.

### 4. `runner/topology.go` and `runner/topology_sink.go`

- `BenchNames` gains `Streams int` and:
  - `IcebergTables(engine string) []string` — one name when `Streams <= 1`
    (unchanged from today), else `_s0.._sN-1` suffixes.
  - `IcebergTableForStream(engine string, i int) string`
- `MatrixRunner` rebuilds names per point via `m.Names.WithStreams(p.Streams)`.
- `MetricSidecar` loops the arm's table list, accumulating `SIZE` and `RECS`
  before emitting the two lines it already emits. `ParseIcebergSeries` is
  untouched.
- `ResetScript` drops and pre-creates every per-stream table (per engine) and
  still resets the shared consumer group to earliest.
- `PipelineForStream(s, n, idx, count)` renders stream `idx`'s input/output with
  that stream's table name; the single-stream path delegates to it with
  `count == 1`.

### 5. `runner/main.go`

- Render one config set per plan point and stage each under `stage/<key>/`:
  - `Streams == 1`: `config.yaml` (unchanged shape).
  - `Streams > 1`: `root.yaml` holding `redpanda`, `http`, `metrics`, `logger`;
    plus `stream-<i>.yaml` each holding only `input`, `buffer`, `output`.
- `stageArtefacts` uploads the whole set and the runner-side download script
  fetches into `/opt/bench/<key>/`.
- Validate: arms present ⇒ `--engines` must be exactly `connect`.

### 6. `runner/render.go` and `runner/templates/result.md.tmpl`

- `PointResult` gains `Arm string`.
- Group rows by `(vcpu, arm)` instead of `vcpu`; add an Arm column.
- Suppress the `Δ vs Connect` column when arms are present (no KC row exists).

### 7. New scenario

`benchmarking/aws/scenarios/iceberg/orders-sink-streams-ab.yaml` — Recipe A base,
`cpu_points: [2]`, the three arms above, `initial_rows: 100000000`.

## Dataset sizing

**100,000,000 rows × 1200 B ≈ 120 GB.**

The window is 15 min at 0 s warmup, so the topic must hold at least
`900 s × peak MB/s`. At 80M rows (96 GB) the topic drains at anything above
107 MB/s — which is precisely the outcome under test, and a drained topic
silently deflates arm B's mean rather than failing loudly. 100M rows covers up to
133 MB/s and still trims 37% off the 160M-row sweep dataset.

If a smaller dataset is preferred, the equivalent safe combination is 80M rows
with a 10-minute window.

## Warmup

Warmup stays at **0 s**, matching the published sweep. The existing validator
enforces `minWarmup = 2m` on any scenario declaring a `workload:` block, and sink
scenarios deliberately omit that block — so there is no way to ask for 60 s
without a new knob, and a new knob is not worth it here.

Arm B therefore absorbs one consumer-group rebalance (seconds out of 900). That
is arguably a genuine cost of the 2-pipeline topology. If it needs removing, the
leading samples can be trimmed from the result JSON afterwards at no cost.

## Verification

Free, before any AWS spend:

- `task aws:validate scenario=iceberg/orders-sink-streams-ab`
- Lint the generated arm-B configs locally. Streams mode requires
  `redpanda`/`http`/`metrics`/`logger` in the `-o` root config and only
  `input`/`buffer`/`output` per stream file; confirm on the laptop, not at
  ~$4/hour.
- `task test:unit -- benchmarking/aws/runner`, including the no-arms parity test.

Acceptance checks on the run itself:

- Each arm's iceberg series must not plateau before window end. A plateau means
  the topic drained and the arm's mean is deflated — rerun with more rows.
- Both arm-B tables must show non-zero growth. A zero means one stream received
  no partitions from the rebalance.
- Arm B's `sweep-*.log` must show two distinct stream IDs.

## Cost

~1.5 h wall clock, ~$6-8: one infra spin-up, one seed of 120 GB, three 15-minute
windows with a reset between arms, one teardown.

## Out of scope

- Kafka Connect comparison — this is a Connect-internal topology question.
- vCPU points other than 2.
- Shared-table variant of arm B (commit-conflict measurement).
- Arms for `direction: source` scenarios.
