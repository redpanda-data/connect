# Bench Bot Part 1: Results Catalogue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A durable, queryable catalogue of Connect benchmark results that answers "is there a current result for this pipeline and workload, and how many CUs does it need?" as JSON a Slack agent can relay.

**Architecture:** A new `catalog` Go package in the `benchmarking/aws` module defines the entry schema, config fingerprinting, matching, staleness and CU sizing. The runner writes one entry per completed bench sweep to a new persistent S3 bucket. A `catalog` CLI exposes `query`, `recommend` and `import` (backfill) with JSON output. Part 2 (the agent) wraps that CLI as tools. Parts 2–4 get their own plans.

**Tech Stack:** Go (module `github.com/redpanda-data/connect/v4/benchmarking/aws`), testify, aws-sdk-go-v2 S3, Terraform (persistent stack), git CLI.

**Spec:** `docs/superpowers/specs/2026-11-20-bench-bot-design.md` (sections "Decisions made", "Constraints", "Design §1 Results catalogue").

## Global Constraints

- New `.go` files carry the RCL licence header used by every file in `benchmarking/aws/runner` (copy from `runner/summary.go` lines 1–7).
- `task lint` / `task fmt` do not cover this module. Run `gofumpt -l .` and `go vet ./...` from `benchmarking/aws` by hand. `go mod tidy` must leave `go.mod`/`go.sum` unchanged (CI enforces it).
- Tests: `cd benchmarking/aws && go test ./catalog/... ./runner/... ./cmd/catalog/...`. No test touches AWS. S3 is behind an interface.
- **1 CU = 0.1 vCPU + 400 MB memory. 1 vCPU = 10 CUs = 4 GB.** MB/GB are decimal here: 400 MB = 400,000,000 bytes, 4 GB = 4,000,000,000 bytes.
- **Headroom = 0.70.** Size so the workload uses at most 70% of measured capacity. It lives in `catalog/sizing.json` (owned by the Connect PM), never as a Go constant.
- **Measured capacity** at a sweep point = `Summary.MedianMBPerSec` of that point. The bench drives the source at saturation, so the median is the sustained ceiling. The answer field is named `capacity_mb_s`. Plan decision, since the spec says "peak". `PeakMBPerSec` is a single-second max and too noisy to sell on.
- Between measured points, interpolate linearly and round **up to the next multiple of 10 CUs**. Below the 10 CU point: answer "10 CUs or less" (`below_floor`). Never extrapolate.
- Every answer carries the Connect version (`connect_version`) and build SHA.
- Bench results only: soak runs (`soak: true`) and arm/binary A/B runs are **never** catalogued (they are not ceiling measurements).
- Catalogue bucket name: `redpanda-connect-bench-catalog`. Key layout: `catalog/v1/<input>/<entry_id>.json`.

## Review Focus

1. **Target beyond the measured ceiling** (need > capacity at the largest point): expect `exceeds_ceiling: true`, `cus: 0` and the ceiling stated. Never an extrapolated number. Test in Task 6.
2. **Non-monotonic sweep** (8 vCPU measured lower than 4 vCPU, which happens with source-bound benches): expect the smallest point whose capacity meets the need. Interpolation only between adjacent points where capacity rises. Test in Task 6.
3. **Entries without memory data** (backfilled, or `memory_limited: false`): the memory bound is reported as `"not_measured"`, never as 0 CUs, and adds caveat `memory_not_limited`. Test in Task 6.
4. **Unknown build SHA** (`git_sha: "unknown"`, which `gitSHA()` returns on failure): expect staleness `unknown` with reason `build sha unknown`, treated as stale. The importer skips such results. Tests in Tasks 5 and 7.
5. **Same pipeline, different hosts/secrets/table names**: identical fingerprint. **Different batching or mapping**: different fingerprint. Test in Task 2.

---

### Task 1: Entry schema

**Files:**
- Create: `benchmarking/aws/catalog/entry.go`
- Test: `benchmarking/aws/catalog/entry_test.go`

**Interfaces:**
- Produces:
  ```go
  const SchemaVersion = 1
  type Entry struct {
      SchemaVersion   int          `json:"schema_version"`
      ID              string       `json:"id"`               // bench session ID
      Scenario        string       `json:"scenario"`         // "postgres/orders-cdc"
      Input           string       `json:"input"`            // component name, e.g. "postgres_cdc"
      Processors      []string     `json:"processors"`       // component names in order
      Output          string       `json:"output"`
      Fingerprint     string       `json:"fingerprint"`      // Task 2
      Workload        Workload     `json:"workload"`
      ConnectVersion  string       `json:"connect_version"`  // `git describe` output, e.g. "v4.118.0-3-gabc1234"
      BuildSHA        string       `json:"build_sha"`
      Infra           Infra        `json:"infra"`
      SinkMeasured    bool         `json:"sink_measured"`
      ExternalStubbed bool         `json:"external_calls_stubbed"`
      MemoryLimited   bool         `json:"memory_limited"`   // hard per-point limit applied (Task 4)
      Points          []Point      `json:"points"`
      AWSCostUSD      float64      `json:"aws_cost_usd"`
      SaaSCostUSD     float64      `json:"saas_cost_usd"`
      RunURL          string       `json:"run_url,omitempty"`
      ResultKey       string       `json:"result_key,omitempty"`
      RecordedAt      time.Time    `json:"recorded_at"`
  }
  type Workload struct {
      MessageSizeBytes  int    `json:"message_size_bytes"`
      Mode              string `json:"mode"` // "cdc" | "bulk"
      OfferedRatePerSec int    `json:"offered_rate_per_sec"`
  }
  type Infra struct {
      RunnerInstanceType string `json:"runner_instance_type"`
      Region             string `json:"region"`
      SinkSize           string `json:"sink_size,omitempty"`
  }
  type Point struct {
      VCPU          float64 `json:"vcpu"`           // float: part 2 adds 0.5
      CUs           int     `json:"cus"`            // VCPU*10
      CapacityMBps  float64 `json:"capacity_mb_s"`
      CapacityMsgps float64 `json:"capacity_msg_s"`
      PeakRSSBytes  uint64  `json:"peak_rss_bytes"` // 0 = not measured
  }
  func (e Entry) Validate() error
  func ModeFor(input string) string // "cdc" if input has suffix "_cdc", else "bulk"
  ```

- [ ] **Step 1: Write failing tests**
  - `TestEntryJSONRoundTrip`: marshal → unmarshal of a fully populated entry is `require.Equal`.
  - `TestEntryValidate`: table-driven. Valid entry → nil. Each of these errors with a message naming the field: `SchemaVersion != 1`, empty `ID`, empty `Input`, empty `Output`, empty `Fingerprint`, empty `BuildSHA`, `Workload.Mode` not in {cdc, bulk}, no points, a point with `CUs != int(VCPU*10)`, duplicate VCPU across points.
  - `TestModeFor`: `postgres_cdc`→cdc, `mongodb_cdc`→cdc, `aws_dynamodb_cdc`→cdc, `kafka_franz`→bulk, `generate`→bulk.
- [ ] **Step 2: Run** `go test ./catalog/ -run 'TestEntry|TestModeFor' -v`. Expect FAIL (package missing).
- [ ] **Step 3: Implement** `entry.go` per the Interfaces block.
- [ ] **Step 4: Run** the same command. Expect PASS.
- [ ] **Step 5: Commit** `bench(catalog): entry schema v1`.

### Task 2: Config fingerprint

**Files:**
- Create: `benchmarking/aws/catalog/fingerprint.go`
- Test: `benchmarking/aws/catalog/fingerprint_test.go`

**Interfaces:**
- Produces:
  ```go
  // Components returns component names for input, pipeline.processors (in
  // order, recursing into nothing: top-level processor names only) and output.
  // Accepts a pipeline map with keys "input", optional "pipeline"
  // ({"processors": [...]}), and "output".
  func Components(pipeline map[string]any) (input string, processors []string, output string, err error)
  // Fingerprint is the hex sha256 of the canonical JSON of the normalised pipeline.
  func Fingerprint(pipeline map[string]any) (string, error)
  ```
- Normalisation (plan decision; the spec leaves "same pipeline" rules to the builder):
  - Numbers and bools are kept.
  - Strings are replaced by `""` **unless** their key is in the keep-list: `mapping`, `check`, `compression`, `codec`, `scanner`, `format`, `period`, `byte_size`, `max_in_flight`, `checkpoint_limit`, `stream_snapshot`, `snapshot_batch_size`, `consumer_group`. Keep the list as an exported `var KeepStringKeys` with a one-line comment per entry.
  - Lists keep order. Maps are serialised with sorted keys (`encoding/json` does this for `map[string]any`).
  - Keys `label`, `tls`, `sasl`, `auth`, `credentials`, `password`, `user`, `username`, `dsn`, `url`, `urls`, `addresses`, `seed_brokers`, `topics`, `tables`, `schema` are dropped entirely, since they are identity, not performance.
  - `${...}` env interpolations are treated as ordinary strings.

- [ ] **Step 1: Write failing tests**
  - `TestFingerprintIgnoresIdentity`: two postgres_cdc pipelines differing only in `dsn`, `tables`, `slot_name` value, `tls.skip_cert_verify` and output `topic` → equal fingerprints.
  - `TestFingerprintSeesPerformanceKnobs`: changing `batching.count` 5000→1000 → different. Changing a `mapping` processor's text → different. Changing `stream_snapshot` → different. Adding a processor → different.
  - `TestComponents`: `{input:{postgres_cdc:{}}, pipeline:{processors:[{mapping:"x"},{branch:{}}]}, output:{snowflake_streaming:{}}}` → `postgres_cdc`, `[mapping branch]`, `snowflake_streaming`. Missing input or output → error. An input map with two keys → error.
- [ ] **Step 2: Run** `go test ./catalog/ -run 'TestFingerprint|TestComponents' -v`. Expect FAIL.
- [ ] **Step 3: Implement** `fingerprint.go`.
- [ ] **Step 4: Run** the same command. Expect PASS.
- [ ] **Step 5: Commit** `bench(catalog): pipeline fingerprint and component extraction`.

### Task 3: Store (S3 and local directory)

**Files:**
- Create: `benchmarking/aws/catalog/store.go`, `benchmarking/aws/catalog/store_s3.go`
- Test: `benchmarking/aws/catalog/store_test.go`
- Modify: `benchmarking/aws/terraform/persistent/gha.tf` (add bucket next to `aws_s3_bucket.soak_archive`, line ~77), `benchmarking/aws/terraform/persistent/outputs.tf`

**Interfaces:**
- Produces:
  ```go
  type Store interface {
      Put(ctx context.Context, e Entry) error                     // Validate()s first; key = Key(e)
      List(ctx context.Context, input string) ([]Entry, error)    // input "" = all
  }
  func Key(e Entry) string // "catalog/v1/<input>/<id>.json"
  func NewDirStore(root string) Store
  type S3API interface { // subset of *s3.Client
      PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
      GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
      ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
  }
  func NewS3Store(api S3API, bucket string) Store
  ```
- Terraform: `aws_s3_bucket "bench_catalog"` named `redpanda-connect-bench-catalog`, versioning enabled, public access blocked, **no** `force_destroy`, same tags as `soak_archive`. Grant the existing bench runner and GHA roles `s3:PutObject`/`GetObject`/`ListBucket` on it, mirroring the `soak_archive` grants in the same file. Output `bench_catalog_bucket`.

- [ ] **Step 1: Write failing tests** (against `NewDirStore(t.TempDir())` and `NewS3Store` with an in-memory fake `S3API`; one shared table runs both):
  - `TestStorePutList`: put 3 entries across 2 inputs. `List(ctx,"postgres_cdc")` returns the 2 postgres entries. `List(ctx,"")` returns all 3.
  - `TestStorePutRejectsInvalid`: an entry failing `Validate` → error, nothing written.
  - `TestStoreListPaginates` (S3 only): the fake returns 2 pages. All entries are returned.
  - `TestStoreListSkipsUnknownSchema`: an object with `schema_version: 2` is skipped, not an error.
- [ ] **Step 2: Run** `go test ./catalog/ -run TestStore -v`. Expect FAIL.
- [ ] **Step 3: Implement** both stores and the Terraform resources.
- [ ] **Step 4: Run** the tests, expect PASS. Then `cd terraform/persistent && terraform fmt -check && terraform validate`, expect success.
- [ ] **Step 5: Commit** `bench(catalog): S3 and directory stores, persistent catalogue bucket`.

### Task 4: Hard per-point memory limit

**Files:**
- Modify: `benchmarking/aws/runner/scenario.go` (infra block), `benchmarking/aws/runner/matrix.go` (`benchScriptArgs` ~line 671, `renderBenchScript` ~line 806, `MatrixRunner.Run` ~line 236)
- Test: `benchmarking/aws/runner/matrix_test.go`, `benchmarking/aws/runner/scenario_test.go`

**Interfaces:**
- Produces: scenario field `infra.memory_per_vcpu_bytes` (int64, yaml `memory_per_vcpu_bytes`, default 0 = off). `benchScriptArgs.MemoryMaxBytes int64` = `memory_per_vcpu_bytes × VCPU`. `Scenario.MemoryLimited() bool` (true when > 0).
- Behaviour: when `MemoryMaxBytes > 0`, the launch line is prefixed with `systemd-run --scope --quiet -p MemoryMax=<bytes> -p MemorySwapMax=0`, before `taskset`. Existing scenarios render byte-identical scripts (default 0).

- [ ] **Step 1: Write failing tests**
  - `TestRenderBenchScript_MemoryMaxWhenSet`: VCPU 4, `MemoryMaxBytes` 16,000,000,000 → script contains `systemd-run --scope --quiet -p MemoryMax=16000000000 -p MemorySwapMax=0 taskset -c 2-5`.
  - `TestRenderBenchScript_NoMemoryMaxByDefault`: `MemoryMaxBytes` 0 → script does not contain `systemd-run`.
  - `TestScenarioMemoryPerVCPUValidate`: a negative value → validation error.
- [ ] **Step 2: Run** `go test ./runner/ -run 'MemoryMax|MemoryPerVCPU' -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./runner/...`. Expect PASS, including all existing `TestRenderBenchScript_*`.
- [ ] **Step 5: Commit** `bench(runner): optional hard per-point memory limit via systemd scope`.

### Task 5: Runner writes catalogue entries

**Files:**
- Create: `benchmarking/aws/catalog/fromrun.go`, `benchmarking/aws/runner/catalogentry.go`
- Test: `benchmarking/aws/catalog/fromrun_test.go`, `benchmarking/aws/runner/catalogentry_test.go`
- Modify: `benchmarking/aws/runner/render.go` (`Result` gains `ConnectVersion string \`json:"connect_version,omitempty"\``), `benchmarking/aws/runner/main.go` (set `ConnectVersion` beside `GitSHA` at ~line 532; add `-catalog-bucket` flag defaulting to `redpanda-connect-bench-catalog`; upload after `WriteResultJSON`)

**Interfaces:**
- Consumes: `catalog.Entry`, `catalog.Components`, `catalog.Fingerprint`, `catalog.ModeFor`, `catalog.Store` (Tasks 1–3). `Scenario.MemoryLimited()` (Task 4).
- Produces:
  ```go
  // in package catalog — the one mapping shared by the runner and the importer (Task 8)
  type RunPoint struct { VCPU int; Engine, Arm, Binary string; MedianMBps, MedianMsgps float64; RSSBytes []uint64 }
  type RunInput struct {
      ID, Scenario, GitSHA, ConnectVersion, InstanceType, Region, RunURL, ResultKey string
      Pipeline          map[string]any // {"input":..., "pipeline":{"processors":...}, "output":...}
      MessageSizeBytes  int
      OfferedRatePerSec int
      Soak, MemoryLimited bool
      AWSCostUSD        float64
      Points            []RunPoint
      Now               time.Time
  }
  func EntryFromRun(r RunInput) (Entry, error) // errors "build sha unknown", "soak result", "arm/binary result", "runner instance type missing"

  // in package main (runner)
  // gitDescribe returns `git -C repoRoot describe --tags --match 'v4.*'`, or "unknown".
  func gitDescribe(repoRoot string) string
  type catalogMeta struct { SessionID, RunURL, ResultKey string; AWSCostUSD float64; Now time.Time }
  // catalogEntryFromResult adapts Scenario + Result into catalog.RunInput and calls catalog.EntryFromRun.
  func catalogEntryFromResult(s *Scenario, input, output map[string]any, r *Result, meta catalogMeta) (catalog.Entry, error)
  // shouldCatalogue: false for soak scenarios and for any result with an arm or binary on any point.
  func shouldCatalogue(s *Scenario, r *Result) bool
  ```
- Mapping:
  - **Points:** one per `PointResult` with engine `connect` or `""`. `VCPU` = `p.VCPU`, `CUs` = `VCPU*10`, `CapacityMBps` = `p.Summary.MedianMBPerSec`, `CapacityMsgps` = `p.Summary.MedianMsgPerSec`, `PeakRSSBytes` = max `RSSBytes` over `p.Prom`.
  - **Workload:** `Workload.MessageSizeBytes` = `r.Dataset.RowSizeBytes`. `OfferedRatePerSec` = scenario `workload.write_rate_per_sec`.
  - **Flags:** `SinkMeasured` = true (the source topology's Redpanda output is real). `ExternalStubbed` = false. `MemoryLimited` = `s.MemoryLimited()`.
- Upload failure prints `warning: catalogue entry: <err>` to stderr and does not fail the run, the same policy as the soak archive upload at main.go ~line 601.

- [ ] **Step 1: Write failing tests**
  - `TestCatalogEntryFromResult`: built from `testdata` scenario `postgres/orders-cdc` plus a 4-point `Result` → input `postgres_cdc`, output `redpanda`, processors `[benchmark]`, mode `cdc`, 4 points with CUs 10/20/40/80, capacity = each median, `PeakRSSBytes` = the max of the fixture's prom RSS, `MemoryLimited` false, `Validate()` nil.
  - `TestEntryFromRun` (catalog package): the `EntryFromRun` error cases listed in Interfaces, one row each.
  - `TestShouldCatalogue`: bench → true. `soak: true` → false. Any point with `Arm` set → false. Any point with `Binary` set → false.
  - `TestCatalogEntryUnknownSHA`: `r.GitSHA == "unknown"` → error `build sha unknown` (Review Focus 4). No entry is written.
- [ ] **Step 2: Run** `go test ./catalog/ -run TestEntryFromRun -v && go test ./runner/ -run 'Catalog|ShouldCatalogue' -v`. Expect FAIL.
- [ ] **Step 3: Implement** and wire into `runBench` after `WriteResultJSON`, gated by `shouldCatalogue`.
- [ ] **Step 4: Run** `go test ./runner/...`. Expect PASS.
- [ ] **Step 5: Commit** `bench(runner): write a catalogue entry for every completed bench sweep`.

### Task 6: Matching and CU recommendation

**Files:**
- Create: `benchmarking/aws/catalog/match.go`, `benchmarking/aws/catalog/sizing.go`, `benchmarking/aws/catalog/sizing.json`
- Test: `benchmarking/aws/catalog/match_test.go`, `benchmarking/aws/catalog/sizing_test.go`

**Interfaces:**
- Consumes: `Entry`, `Point` (Task 1).
- Produces:
  ```go
  type Query struct {
      Input, Output     string
      Processors        []string
      Fingerprint       string // "" when the request had no config
      MessageSizeBytes  int
      Mode              string
  }
  type MatchKind string // "exact" | "close"
  type Match struct { Entry Entry; Kind MatchKind; Score float64; Differences []string }
  // Match returns candidates best-first.
  //   exact: same Input, Output, Mode, Fingerprint (when the query has one), and message size within ±10%.
  //   close: same Input and Output.
  // Close score = |ln(entrySize/querySize)| + 0.5*(processor set symmetric difference count) + 1 if mode differs; lower is better.
  // Ties are broken by newer RecordedAt. Differences lists human-readable deltas ("message size 1200 vs 1024 bytes").
  func FindMatches(entries []Entry, q Query) []Match

  type SizingConfig struct { Headroom float64 `json:"headroom"`; CUsPerVCPU int `json:"cus_per_vcpu"`; BytesPerCU int64 `json:"bytes_per_cu"` }
  func LoadSizingConfig(path string) (SizingConfig, error) // validates 0<headroom<=1, positive ints
  type Recommendation struct {
      CUs            int      `json:"cus"`            // 0 when below_floor or exceeds_ceiling
      VCPU           float64  `json:"vcpu"`
      BoundBy        string   `json:"bound_by"`       // "throughput" | "memory"
      Estimated      bool     `json:"estimated"`      // interpolated between points
      BelowFloor     bool     `json:"below_floor"`
      ExceedsCeiling bool     `json:"exceeds_ceiling"`
      CeilingMBps    float64  `json:"ceiling_mb_s"`
      MemoryBound    string   `json:"memory_bound"`   // "<n> CUs" | "not_measured"
      Headroom       float64  `json:"headroom"`
      Caveats        []string `json:"caveats"`        // e.g. "memory_not_limited", "sink_not_measured", "external_calls_stubbed"
  }
  func Recommend(e Entry, targetMBps float64, cfg SizingConfig) (Recommendation, error)
  ```
- `sizing.json`: `{"headroom": 0.70, "cus_per_vcpu": 10, "bytes_per_cu": 400000000}`.
- Recommend algorithm (the tests pin it):
  1. `need = target / headroom`.
  2. Sort points by VCPU.
  3. If `need <= cap(first)`, return `BelowFloor`.
  4. Find the first point `j` with `cap(j) >= need`. None → `ExceedsCeiling`, with `CeilingMBps` = max cap.
  5. If `cap(j-1) < cap(j)`, interpolate `vcpu = v(j-1) + (need-cap(j-1))/(cap(j)-cap(j-1))*(v(j)-v(j-1))`. Otherwise use `v(j)`.
  6. `cpuCUs = ceil(vcpu*CUsPerVCPU / 10) * 10`.
  7. `memCUs = ceil(peakRSS(j) / BytesPerCU)` rounded up to a multiple of 10, only when `e.MemoryLimited && peakRSS(j) > 0`. Otherwise `MemoryBound = "not_measured"` and add caveat `memory_not_limited`.
  8. `CUs = max(cpuCUs, memCUs)` and set `BoundBy` to match.
  9. Add caveats from `!SinkMeasured` and `ExternalStubbed`.

- [ ] **Step 1: Write failing tests.** Fixture points: (1 vCPU, 18 MB/s), (2, 35), (4, 70), (8, 120). `MemoryLimited` true. RSS 1.5 GB, 3 GB, 6 GB, 12 GB.
  - `TestRecommendExactPoint`: target 40 → need 57.14 → interpolate between 2 and 4 → vcpu 3.27 → 40 CUs (ceil(32.7/10)*10). `Estimated` true, `BoundBy` throughput.
  - `TestRecommendInterpolatesToThirty`: target 30 → need 42.86 → vcpu 2.45 → 30 CUs.
  - `TestRecommendBelowFloor`: target 10 → need 14.3 ≤ 18 → `BelowFloor`, `CUs` 0.
  - `TestRecommendExceedsCeiling` (Review Focus 1): target 100 → need 142.9 > 120 → `ExceedsCeiling`, `CeilingMBps` 120, `CUs` 0.
  - `TestRecommendNonMonotonic` (Review Focus 2): points (1,18),(2,35),(4,70),(8,60). Target 45 → need 64.3 → j = 4 vCPU → interpolate between 2 and 4 → 40 CUs. Target 49.5 → need 70.7 → exceeds (no point ≥ 70.7) → `ExceedsCeiling`, ceiling 70.
  - `TestRecommendMemoryBound`: the same fixture with the 4-vCPU RSS at 20 GB, target 40 → memCUs 50 → `CUs` 50, `BoundBy` memory.
  - `TestRecommendMemoryNotMeasured` (Review Focus 3): `MemoryLimited` false → `MemoryBound` "not_measured", caveat `memory_not_limited`, CUs from throughput only.
  - `TestRecommendRejectsBadTarget`: target 0 or negative → error.
  - `TestFindMatches`: an exact match ranks first. A 2048-byte entry is `close` with a difference string naming both sizes. A different output is excluded. A newer entry wins a tie.
  - `TestLoadSizingConfig`: the shipped `sizing.json` loads to {0.70, 10, 400000000}. Headroom 0 or 1.5 → error.
- [ ] **Step 2: Run** `go test ./catalog/ -run 'TestRecommend|TestFindMatches|TestLoadSizingConfig' -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit** `bench(catalog): match entries and recommend CUs with 70% headroom`.

### Task 7: Staleness

**Files:**
- Create: `benchmarking/aws/catalog/staleness.go`
- Test: `benchmarking/aws/catalog/staleness_test.go`

**Interfaces:**
- Consumes: `Entry` (Task 1).
- Produces:
  ```go
  type Git interface {
      LatestRelease(ctx context.Context) (tag string, err error)                        // highest vX.Y.Z tag: git tag --list 'v4.*' --sort=-v:refname
      ComponentPaths(ctx context.Context, component string) ([]string, error)           // dirs whose .go files contain the string literal "<component>" in a Register* call: git grep -l -E 'Register[A-Za-z]*\("<component>"' -- internal/impl, mapped to their directories
      ChangedPaths(ctx context.Context, fromSHA, toRef string, paths []string) ([]string, error) // git log --format=%h fromSHA..toRef -- paths
      ChangelogMentions(ctx context.Context, fromSHA, toRef, component string) ([]string, error) // CHANGELOG.md lines added in the range that start "- <component>:" or contain "`<component>`"
  }
  func NewGitCLI(repoRoot string) Git
  type Staleness struct {
      State         string   `json:"state"`          // "current" | "stale" | "unknown"
      LatestRelease string   `json:"latest_release"`
      Reasons       []string `json:"reasons"`        // e.g. "3 commits to internal/impl/postgresql since abc1234", "CHANGELOG: postgres_cdc: ..."
  }
  // CheckStaleness checks Input, Output and each processor component.
  func CheckStaleness(ctx context.Context, g Git, e Entry) (Staleness, error)
  ```
- Rules:
  - `e.BuildSHA` empty or `"unknown"` → `unknown` with reason `build sha unknown`.
  - Any changed path or changelog mention for any component → `stale` with reasons.
  - Otherwise `current`.
  - Processors like `mapping` resolve to their own dirs too. A component that resolves to no paths adds reason `no source path for <c>` but does not by itself make the entry stale.

- [ ] **Step 1: Write failing tests** with a fake `Git`:
  - current: no changes.
  - stale via path: changes in `internal/impl/postgresql`.
  - stale via changelog only.
  - unknown SHA (Review Focus 4).
  - unresolvable component: `current`, with the reason recorded.
  - `TestGitCLIAgainstRepo`: runs `NewGitCLI` against the repo itself. `LatestRelease` matches `^v4\.\d+\.\d+$`, and `ComponentPaths("postgres_cdc")` contains `internal/impl/postgresql`. Skip with `t.Skip` when `git` is unavailable.
- [ ] **Step 2: Run** `go test ./catalog/ -run Staleness -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit** `bench(catalog): staleness against the latest release`.

### Task 8: `catalog` CLI (query, recommend, import)

**Files:**
- Create: `benchmarking/aws/cmd/catalog/main.go`, `benchmarking/aws/cmd/catalog/import.go`
- Test: `benchmarking/aws/cmd/catalog/main_test.go`, `benchmarking/aws/cmd/catalog/import_test.go`
- Modify: `benchmarking/aws/Taskfile.yml` (add `catalog:query`, `catalog:recommend`, `catalog:import` tasks passing `CLI_ARGS`)

**Interfaces:**
- Consumes: everything in Tasks 1–3, 6 and 7.
- Produces:
  - **Shared flags:** `--store s3://<bucket>` or `--store dir:<path>`, default `s3://redpanda-connect-bench-catalog`. `--repo <path>` for staleness.
  - `catalog query --input X --output Y [--processors a,b] [--config file.yaml] [--message-size N] [--mode cdc|bulk]`
    - Prints JSON `{"matches":[{"kind","score","differences","entry","staleness"}]}`, at most 5, best first.
    - `--config` derives input/output/processors/fingerprint via `catalog.Components`/`catalog.Fingerprint` and overrides those flags.
  - `catalog recommend --entry-id ID --target-mb-s F [--sizing path]`
    - Prints JSON `{"entry_id","connect_version","build_sha","instance_type","sink_size","recommendation","staleness","points"}`.
  - `catalog import --results-root DIR --repo PATH --scenarios-root DIR`
    - Walks `<root>/<connector>/<scenario>/*.json` (the `walkResults` layout in `runner/summary.go`) and builds entries.
    - Prints JSON `{"imported":[ids],"skipped":[{"path","reason"}]}`.
  - Exit code 0 with empty results is not an error. Bad flags exit 2.
- Import rules:
  - Skip with a reason when: `git_sha` is empty/`unknown`; `infra.runner_instance_type` is empty; the scenario YAML is missing under `--scenarios-root`; the result is a soak or arm/binary result.
  - `ConnectVersion` = `git -C <repo> describe --tags --match 'v4.*' <git_sha>`. Skip with reason when it fails.
  - `MemoryLimited` = false, unless the scenario sets `memory_per_vcpu_bytes`.
  - Build entries with `catalog.EntryFromRun` (Task 5), so there is one mapping. Entry `ID` = `<connector>-<scenario>-<result file stamp>`, e.g. `postgres-orders-cdc-2026-06-01T10-00-00Z`, since `Result` carries no session ID.

- [ ] **Step 1: Write failing tests** using `dir:` stores and `testdata` fixtures:
  - `TestQueryJSON`: two stored entries. `query --input postgres_cdc --output redpanda --message-size 1200 --mode cdc` → the first match has kind `exact`, and the output parses as the documented JSON.
  - `TestQueryWithConfig`: `--config` with a different `dsn` still matches exactly.
  - `TestRecommendJSON`: the Task 6 fixture, target 40 → `recommendation.cus` 40 and `connect_version` present.
  - `TestImportSkipsAndImports`: a fixture root with one good result, one `git_sha: "unknown"`, one with no instance type and one soak → 1 imported, 3 skipped with the reasons `build sha unknown`, `runner instance type missing`, `soak result`.
  - `TestBadFlagsExit2`.
- [ ] **Step 2: Run** `go test ./cmd/catalog/ -v`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./catalog/... ./runner/... ./cmd/catalog/...`, `go vet ./...`, `gofumpt -l .` (no output) and `go mod tidy && git diff --exit-code go.mod go.sum`. All clean.
- [ ] **Step 5: Commit** `bench(catalog): query, recommend and import CLI`.

### Task 9: Deploy and backfill (operator task, needs AWS access)

**Files:** none (operational).

- [ ] **Step 1:** Apply the persistent stack: `cd benchmarking/aws && task aws:persistent`. Verify with `aws s3api head-bucket --bucket redpanda-connect-bench-catalog`.
- [ ] **Step 2:** Import the existing postgres/mysql bench results from this repo's results directory, then from the `connect_prakhar` checkout: `task catalog:import -- --store s3://redpanda-connect-bench-catalog --results-root <connect_prakhar>/benchmarking/aws/results --repo <connect_prakhar> --scenarios-root <connect_prakhar>/benchmarking/aws/scenarios`. Save the JSON output.
- [ ] **Step 3:** Report the imported count and every skip reason to the Connect PM. Skipped results stay out by design (the spec: "left out, not guessed").
- [ ] **Step 4:** Smoke test: `task catalog:query -- --input postgres_cdc --output redpanda --message-size 1200 --mode cdc` returns at least one match, and `catalog recommend` on it returns a CU figure with `connect_version` set.

---

## Later plans (not in this one)

- **Part 2, agent:** ADP agent, Slack relay with secret redaction, wrapping this CLI as tools, cost estimate, thumbs-up/"go" approval, workflow dispatch.
- **Part 3, parallel runs:** separate AWS account, session-scoped Terraform state, per-run workflow concurrency.
- **Part 4, connectors:** end-to-end topology with real sinks (Snowflake, BigQuery, S3, Iceberg on S3, Redpanda), null-sink fallback, fork stack ports, 0.5 vCPU point, sink reference sizes.
