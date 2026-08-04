# Iceberg streams A/B (`matrix.arms`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a sink-only `matrix.arms` sweep dimension to the AWS bench runner so three launch topologies (1 pipeline @ `GOMAXPROCS=2`, 1 pipeline @ `GOMAXPROCS=4`, 2 streams-mode pipelines @ `GOMAXPROCS=4`) can be measured at 2 vCPU in a single session against one seeded dataset, then add the iceberg scenario that uses it.

**Architecture:** A new `sweepPoint` plan list replaces the raw `[]int` cpu-points loop in `MatrixRunner.Run`. Each point carries its own `GOMAXPROCS`, stream count, and merged pipeline config. Per-stream Iceberg table naming is threaded entirely through `BenchNames` (which gains `Streams`/`StreamIndex` fields), so `sinkTopology.Pipeline` and the `Topology` interface need no changes. All new machinery is gated behind a non-empty `matrix.arms`: scenarios without arms keep byte-identical S3 keys, artifact names, config staging paths, and rendered config *content*. Two intentional deviations exist in the rendered script *text*, neither parsed by anything: the informational `starting bench:` echo line gains GOMAXPROCS and stream counts, and the reset wraps `iceberg-tablegen` in a bounded retry.

**Tech Stack:** Go 1.x, `gopkg.in/yaml.v3`, `github.com/stretchr/testify/require`, AWS SSM/S3/Glue, `redpanda-connect streams` CLI mode.

**Spec:** `docs/superpowers/specs/2026-08-03-iceberg-streams-ab-design.md`

## Global Constraints

- Every new or modified `.go` file keeps the existing 4-line BSL license header verbatim:
  ```go
  // Copyright 2025 Redpanda Data, Inc.
  //
  // Use of this software is governed by the Business Source License included
  // in the licenses/BSL.md file.
  ```
- All work is in `benchmarking/aws/`. Do not touch `internal/`, `public/`, or `cmd/`.
- Test command: `task test:unit -- benchmarking/aws/runner`. A faster inner loop is `go test ./benchmarking/aws/runner/ -run <TestName> -v`.
- `arms` is valid only for `direction: sink` and only with `--engines=connect`.
- **Parity requirement (non-negotiable):** a scenario with no `matrix.arms` must produce exactly the same rendered bench script, the same S3 artifact keys, and the same `/opt/bench/config.yaml` staging path as before this change. The six existing scenarios must be unaffected. Several tasks assert this directly.
- New per-arm resources are named from the arm ID, which must match `^[a-z0-9][a-z0-9-]*$` because it lands in filenames and S3 keys.
- **`task fmt` and `task lint` do NOT cover `benchmarking/`** — both are scoped to
  `cmd/... internal/... public/...` (`Taskfile.yml:40,49`). Format with
  `gofmt -l -w` on **only the files the task changed** and confirm
  `go vet ./benchmarking/aws/runner/` is clean before each commit. Do NOT run
  `gofmt -w` over the whole package: `benchmarking/aws/runner/doc.go` and
  `ssm.go` carry pre-existing formatting drift unrelated to this plan, and
  reformatting them would smuggle unrelated churn into a task's diff.
- Commit after every task. Branch: `benchmarking`.

---

### Task 1: `Arm` type and scenario validation

**Files:**
- Modify: `benchmarking/aws/runner/scenario.go:96-100` (`MatrixSpec`), and `Validate()` around `:256-268`
- Create: `benchmarking/aws/runner/testdata/valid-iceberg-arms.yaml`
- Create: `benchmarking/aws/runner/testdata/invalid-arms-source.yaml`
- Create: `benchmarking/aws/runner/testdata/invalid-arms-multi-cpu.yaml`
- Test: `benchmarking/aws/runner/scenario_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `type Arm struct { ID string; GOMAXPROCS int; Streams int; Pipeline map[string]any }` and `MatrixSpec.Arms []Arm`. Later tasks read `s.Matrix.Arms`.

- [ ] **Step 1: Write the failing tests**

Add to `benchmarking/aws/runner/scenario_test.go`:

```go
func TestLoadScenario_ParsesArms(t *testing.T) {
	s, err := LoadScenario("testdata/valid-iceberg-arms.yaml")
	require.NoError(t, err)
	require.Len(t, s.Matrix.Arms, 3)
	require.Equal(t, "a0-1pipe-gmp2", s.Matrix.Arms[0].ID)
	require.Equal(t, 2, s.Matrix.Arms[0].GOMAXPROCS)
	require.Equal(t, 1, s.Matrix.Arms[0].Streams)
	require.Equal(t, "b-2pipe-gmp4", s.Matrix.Arms[2].ID)
	require.Equal(t, 4, s.Matrix.Arms[2].GOMAXPROCS)
	require.Equal(t, 2, s.Matrix.Arms[2].Streams)
	// Per-arm pipeline override is parsed as a nested map, not flattened.
	out, ok := s.Matrix.Arms[2].Pipeline["output"].(map[string]any)
	require.True(t, ok, "arm pipeline override must parse as map[string]any")
	ice, ok := out["iceberg"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 8, ice["max_in_flight"])
}

func TestLoadScenario_RejectsArmsOnSource(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-arms-source.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms")
	require.Contains(t, err.Error(), "sink")
}

func TestLoadScenario_RejectsArmsWithMultipleCPUPoints(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-arms-multi-cpu.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms")
	require.Contains(t, err.Error(), "cpu_points")
}

func TestScenarioValidate_RejectsBadArmID(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "Bad_ID", GOMAXPROCS: 4, Streams: 1}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "matrix.arms[0].id")
}

func TestScenarioValidate_RejectsDuplicateArmIDs(t *testing.T) {
	s := &Scenario{
		Name: "iceberg-x", Connector: "iceberg", Stack: "iceberg",
		Direction: DirectionSink,
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Dataset:   DatasetSpec{InitialRows: 1000, RowSizeBytes: 1200, Seeder: "json-orders", ExpectedPeakMBSec: 200},
		Pipeline:  map[string]any{"output": map[string]any{"iceberg": map[string]any{}}},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "dup", Streams: 1}, {ID: "dup", Streams: 2}},
		},
	}
	err := s.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}
```

Create `benchmarking/aws/runner/testdata/valid-iceberg-arms.yaml`:

```yaml
name: iceberg-orders-sink-streams-ab
description: arms fixture
direction: sink
connector: iceberg
stack: iceberg

infra:
  runner:
    instance_type: c8g.4xlarge

dataset:
  initial_rows: 110000000
  row_size_bytes: 1200
  seeder: json-orders
  expected_peak_mb_s: 133

pipeline:
  buffer:
    memory:
      limit: 524288000
      batch_policy:
        count: 10000
        period: 10s
  output:
    iceberg:
      max_in_flight: 16
      batching:
        count: 10000
        period: 10s
      commit:
        max_snapshot_age: 24h

matrix:
  cpu_points: [2]
  arms:
    - id: a0-1pipe-gmp2
      gomaxprocs: 2
      streams: 1
    - id: a1-1pipe-gmp4
      gomaxprocs: 4
      streams: 1
    - id: b-2pipe-gmp4
      gomaxprocs: 4
      streams: 2
      pipeline:
        buffer:
          memory:
            limit: 262144000
        output:
          iceberg:
            max_in_flight: 8
```

Create `benchmarking/aws/runner/testdata/invalid-arms-source.yaml` — same as above but with `direction: source`, `connector: postgres_cdc`, `stack: postgres`, and this replacing the `pipeline`/`dataset` blocks:

```yaml
name: postgres-arms-bad
description: arms on a source scenario must be rejected
direction: source
connector: postgres_cdc
stack: postgres

infra:
  runner:
    instance_type: c7i.4xlarge
  source:
    instance_class: db.m7i.2xlarge
    storage_gb: 500

dataset:
  initial_rows: 1000000
  row_size_bytes: 256
  tables: [orders]
  seeder: cdc-rows

workload:
  write_rate_per_sec: 1000
  duration: 15m
  warmup: 2m

pipeline:
  input:
    postgres_cdc: {}

matrix:
  cpu_points: [2]
  arms:
    - id: a0
      gomaxprocs: 2
      streams: 1
```

Create `benchmarking/aws/runner/testdata/invalid-arms-multi-cpu.yaml` — a copy of `valid-iceberg-arms.yaml` with `cpu_points: [1, 2]`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run 'TestLoadScenario_ParsesArms|TestLoadScenario_RejectsArms|TestScenarioValidate_Rejects(BadArmID|DuplicateArmIDs)' -v`

Expected: FAIL — `s.Matrix.Arms` undefined (compile error), `Arm` undefined.

- [ ] **Step 3: Add the `Arm` type and the `Arms` field**

In `benchmarking/aws/runner/scenario.go`, replace the `MatrixSpec` struct:

```go
type MatrixSpec struct {
	CPUPoints         []int                  `yaml:"cpu_points"`
	GoMemLimitPerVCPU int                    `yaml:"go_mem_limit_per_vcpu,omitempty"`
	Overrides         map[int]map[string]any `yaml:"overrides,omitempty"`
	// Arms turns a single cpu_points entry into an A/B: each arm is measured
	// at that same vCPU pin but with its own launch topology (GOMAXPROCS,
	// stream count) and pipeline overrides. Sink-only, Connect-only. Empty
	// for every pre-existing scenario, which keeps the classic
	// one-point-per-cpu_points behaviour.
	Arms []Arm `yaml:"arms,omitempty"`
}

// Arm is one leg of an A/B at a fixed vCPU point. GOMAXPROCS may exceed the
// pinned core count deliberately: Connect counts licensed cores off the machine
// CPU rather than GOMAXPROCS, so oversubscribing is free, and an I/O-blocked
// output (e.g. iceberg commits) can leave pinned cores idle without it.
// Streams > 1 launches `redpanda-connect streams` with one config per stream
// instead of `redpanda-connect run`.
type Arm struct {
	ID         string `yaml:"id"`
	GOMAXPROCS int    `yaml:"gomaxprocs,omitempty"`
	Streams    int    `yaml:"streams,omitempty"`
	// Pipeline is deep-merged over the scenario-level pipeline block for this
	// arm, so an arm declares only what differs. Applied to every stream.
	Pipeline map[string]any `yaml:"pipeline,omitempty"`
}
```

- [ ] **Step 4: Add validation**

In `benchmarking/aws/runner/scenario.go`, inside `Validate()`, immediately after the existing `cpu_points` checks (the block ending with the strictly-ascending loop around `:262-266`), insert:

```go
	if len(s.Matrix.Arms) > 0 {
		if s.Direction != DirectionSink {
			return fmt.Errorf("matrix.arms is only supported for direction: sink (got %q)", s.Direction)
		}
		if len(s.Matrix.CPUPoints) != 1 {
			return fmt.Errorf("matrix.arms requires exactly one matrix.cpu_points entry (got %v): arms × vCPU would multiply the run", s.Matrix.CPUPoints)
		}
		seen := map[string]bool{}
		for i, a := range s.Matrix.Arms {
			if !armIDRe.MatchString(a.ID) {
				return fmt.Errorf("matrix.arms[%d].id %q must match %s (it is used in filenames and S3 keys)", i, a.ID, armIDRe.String())
			}
			if seen[a.ID] {
				return fmt.Errorf("matrix.arms[%d].id %q is a duplicate; arm ids must be unique", i, a.ID)
			}
			seen[a.ID] = true
			if a.GOMAXPROCS < 0 {
				return fmt.Errorf("matrix.arms[%d].gomaxprocs must be positive when set (got %d)", i, a.GOMAXPROCS)
			}
			if a.Streams < 0 {
				return fmt.Errorf("matrix.arms[%d].streams must be positive when set (got %d)", i, a.Streams)
			}
		}
	}
```

Add near the top of `scenario.go`, after the `const` block:

```go
// armIDRe constrains arm ids to what is safe in a filename and an S3 key.
var armIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
```

Add `"regexp"` to the import block.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./benchmarking/aws/runner/ -run 'TestLoadScenario|TestScenarioValidate' -v`

Expected: PASS, including all pre-existing `TestLoadScenario_*` cases (proves no regression in validation for the six existing scenarios).

- [ ] **Step 6: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/scenario.go benchmarking/aws/runner/scenario_test.go benchmarking/aws/runner/testdata/
git commit -m "feat(bench): matrix.arms scenario field + validation (sink-only)"
```

---

### Task 2: Sweep plan expansion

**Files:**
- Create: `benchmarking/aws/runner/sweepplan.go`
- Test: `benchmarking/aws/runner/sweepplan_test.go`

**Interfaces:**
- Consumes: `Scenario`, `MatrixSpec.Arms`, `Arm` from Task 1.
- Produces:
  - `type sweepPoint struct { VCPU int; ArmID string; GOMAXPROCS int; Streams int; Pipeline map[string]any }`
  - `func (p sweepPoint) Key() string`
  - `func buildSweepPlan(s *Scenario) []sweepPoint`
  - `func planMaxStreams(plan []sweepPoint) int`
  - `func mergePipeline(base, override map[string]any) map[string]any`

- [ ] **Step 1: Write the failing tests**

Create `benchmarking/aws/runner/sweepplan_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The parity guard: no arms means one point per cpu_point, GOMAXPROCS == VCPU,
// one stream, and a bare-integer Key so every artifact name is unchanged.
func TestBuildSweepPlan_NoArmsIsIdentityOverCPUPoints(t *testing.T) {
	s := &Scenario{Matrix: MatrixSpec{CPUPoints: []int{1, 2, 4, 8}}}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 4)
	for i, want := range []int{1, 2, 4, 8} {
		require.Equal(t, want, plan[i].VCPU)
		require.Equal(t, want, plan[i].GOMAXPROCS, "GOMAXPROCS must default to VCPU")
		require.Equal(t, 1, plan[i].Streams)
		require.Empty(t, plan[i].ArmID)
		require.Nil(t, plan[i].Pipeline, "no arms means no merged pipeline")
	}
	require.Equal(t, []string{"1", "2", "4", "8"}, []string{
		plan[0].Key(), plan[1].Key(), plan[2].Key(), plan[3].Key(),
	})
}

func TestBuildSweepPlan_ArmsExpandAtSingleCPUPoint(t *testing.T) {
	s := &Scenario{
		Pipeline: map[string]any{
			"buffer": map[string]any{"memory": map[string]any{"limit": 524288000}},
			"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 16, "batching": map[string]any{"count": 10000}}},
		},
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms: []Arm{
				{ID: "a0-1pipe-gmp2", GOMAXPROCS: 2, Streams: 1},
				{ID: "a1-1pipe-gmp4", GOMAXPROCS: 4, Streams: 1},
				{ID: "b-2pipe-gmp4", GOMAXPROCS: 4, Streams: 2, Pipeline: map[string]any{
					"buffer": map[string]any{"memory": map[string]any{"limit": 262144000}},
					"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 8}},
				}},
			},
		},
	}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 3)
	require.Equal(t, "2-a0-1pipe-gmp2", plan[0].Key())
	require.Equal(t, "2-b-2pipe-gmp4", plan[2].Key())
	require.Equal(t, 2, plan[0].GOMAXPROCS)
	require.Equal(t, 4, plan[2].GOMAXPROCS)
	require.Equal(t, 2, plan[2].Streams)
	require.Equal(t, 2, plan[0].VCPU, "every arm is measured at the same vCPU pin")

	// Arm 0 inherits the scenario pipeline verbatim.
	buf0 := plan[0].Pipeline["buffer"].(map[string]any)["memory"].(map[string]any)
	require.Equal(t, 524288000, buf0["limit"])

	// Arm 2 overrides the two fields it names and inherits the rest.
	buf2 := plan[2].Pipeline["buffer"].(map[string]any)["memory"].(map[string]any)
	require.Equal(t, 262144000, buf2["limit"])
	ice2 := plan[2].Pipeline["output"].(map[string]any)["iceberg"].(map[string]any)
	require.Equal(t, 8, ice2["max_in_flight"])
	require.Equal(t, map[string]any{"count": 10000}, ice2["batching"], "unnamed sibling keys survive the merge")
}

// The merge must not mutate the scenario or leak shared sub-maps between arms:
// two arms merged from one base must be independently editable.
func TestMergePipeline_DoesNotAliasBase(t *testing.T) {
	base := map[string]any{"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 16}}}
	got := mergePipeline(base, map[string]any{"output": map[string]any{"iceberg": map[string]any{"max_in_flight": 8}}})

	require.Equal(t, 8, got["output"].(map[string]any)["iceberg"].(map[string]any)["max_in_flight"])
	require.Equal(t, 16, base["output"].(map[string]any)["iceberg"].(map[string]any)["max_in_flight"],
		"base must be untouched")

	got["output"].(map[string]any)["iceberg"].(map[string]any)["new"] = true
	_, leaked := base["output"].(map[string]any)["iceberg"].(map[string]any)["new"]
	require.False(t, leaked, "merged result must not share sub-maps with base")
}

func TestMergePipeline_DeepCopiesSlices(t *testing.T) {
	base := map[string]any{"input": map[string]any{"redpanda": map[string]any{"topics": []any{"a"}}}}
	got := mergePipeline(base, nil)
	got["input"].(map[string]any)["redpanda"].(map[string]any)["topics"].([]any)[0] = "mutated"
	require.Equal(t, "a", base["input"].(map[string]any)["redpanda"].(map[string]any)["topics"].([]any)[0])
}

func TestBuildSweepPlan_ArmDefaultsGOMAXPROCSToVCPUAndStreamsToOne(t *testing.T) {
	s := &Scenario{Matrix: MatrixSpec{CPUPoints: []int{2}, Arms: []Arm{{ID: "bare"}}}}
	plan := buildSweepPlan(s)
	require.Len(t, plan, 1)
	require.Equal(t, 2, plan[0].GOMAXPROCS)
	require.Equal(t, 1, plan[0].Streams)
}

func TestPlanMaxStreams(t *testing.T) {
	require.Equal(t, 1, planMaxStreams([]sweepPoint{{Streams: 1}, {Streams: 1}}))
	require.Equal(t, 2, planMaxStreams([]sweepPoint{{Streams: 1}, {Streams: 2}}))
	require.Equal(t, 1, planMaxStreams(nil), "empty plan still yields a usable single-table reset")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run 'TestBuildSweepPlan|TestMergePipeline|TestPlanMaxStreams' -v`

Expected: FAIL — `buildSweepPlan`, `sweepPoint`, `mergePipeline`, `planMaxStreams` undefined.

- [ ] **Step 3: Write the implementation**

Create `benchmarking/aws/runner/sweepplan.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strconv"
)

// sweepPoint is one measured leg of the sweep: a vCPU pin plus the launch
// topology to measure at it. A scenario without matrix.arms produces exactly
// one point per cpu_points entry with GOMAXPROCS == VCPU and a single stream,
// which is byte-for-byte the pre-arms behaviour.
type sweepPoint struct {
	VCPU int
	// ArmID is "" for arm-less scenarios. Non-empty ids appear in artifact
	// names via Key.
	ArmID      string
	GOMAXPROCS int
	Streams    int
	// Pipeline is the scenario pipeline with this arm's overrides merged in.
	// nil for arm-less scenarios, whose callers use Scenario.Pipeline directly.
	Pipeline map[string]any
}

// Key identifies the point in log filenames and S3 keys. Arm-less points key
// off the bare vCPU count so existing scenarios keep their artifact paths.
func (p sweepPoint) Key() string {
	if p.ArmID == "" {
		return strconv.Itoa(p.VCPU)
	}
	return fmt.Sprintf("%d-%s", p.VCPU, p.ArmID)
}

// buildSweepPlan expands matrix.cpu_points × matrix.arms into the ordered list
// of points to measure. Arms are validated to a single cpu_points entry
// (Scenario.Validate), so the nested loop yields len(arms) points in practice.
func buildSweepPlan(s *Scenario) []sweepPoint {
	if len(s.Matrix.Arms) == 0 {
		pts := make([]sweepPoint, 0, len(s.Matrix.CPUPoints))
		for _, n := range s.Matrix.CPUPoints {
			pts = append(pts, sweepPoint{VCPU: n, GOMAXPROCS: n, Streams: 1})
		}
		return pts
	}
	pts := make([]sweepPoint, 0, len(s.Matrix.CPUPoints)*len(s.Matrix.Arms))
	for _, n := range s.Matrix.CPUPoints {
		for _, a := range s.Matrix.Arms {
			gmp := a.GOMAXPROCS
			if gmp == 0 {
				gmp = n
			}
			streams := a.Streams
			if streams == 0 {
				streams = 1
			}
			pts = append(pts, sweepPoint{
				VCPU:       n,
				ArmID:      a.ID,
				GOMAXPROCS: gmp,
				Streams:    streams,
				Pipeline:   mergePipeline(s.Pipeline, a.Pipeline),
			})
		}
	}
	return pts
}

// planMaxStreams is the largest stream count in the plan. The between-points
// reset pre-creates the union of every arm's tables (see
// BenchNames.IcebergResetTables), which lets one precomputed reset script serve
// every arm regardless of its own stream count.
func planMaxStreams(plan []sweepPoint) int {
	max := 1
	for _, p := range plan {
		if p.Streams > max {
			max = p.Streams
		}
	}
	return max
}

// mergePipeline returns a deep copy of base with override's keys merged in
// recursively. Nested maps merge key-by-key; every other value (scalars,
// sequences) is replaced wholesale. Neither argument is mutated and the result
// shares no sub-maps with either, so sibling arms can be edited independently.
func mergePipeline(base, override map[string]any) map[string]any {
	out, _ := deepCopyValue(base).(map[string]any)
	if out == nil {
		out = map[string]any{}
	}
	for k, v := range override {
		if vm, ok := v.(map[string]any); ok {
			if bm, ok := out[k].(map[string]any); ok {
				out[k] = mergePipeline(bm, vm)
				continue
			}
		}
		out[k] = deepCopyValue(v)
	}
	return out
}

// deepCopyValue copies the map/slice spine of a yaml.v3-decoded value. Scalars
// are returned as-is (they are immutable in practice).
func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = deepCopyValue(vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = deepCopyValue(vv)
		}
		return out
	default:
		return v
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./benchmarking/aws/runner/ -run 'TestBuildSweepPlan|TestMergePipeline|TestPlanMaxStreams' -v`

Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/sweepplan.go benchmarking/aws/runner/sweepplan_test.go
git commit -m "feat(bench): sweep plan expansion for matrix.arms with no-arms parity"
```

---

### Task 3: Stream-aware Iceberg table names on `BenchNames`

**Files:**
- Modify: `benchmarking/aws/runner/topology.go:18-55` (`BenchNames` and its methods)
- Test: `benchmarking/aws/runner/topology_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `BenchNames` gains fields `Streams int` and `StreamIndex int`.
  - `func (n BenchNames) WithStreams(count int) BenchNames`
  - `func (n BenchNames) WithStream(idx int) BenchNames`
  - `func (n BenchNames) IcebergTables(engine string) []string`
  - `func (n BenchNames) IcebergResetTables(engine string, maxStreams int) []string`
  - `IcebergTable(engine string) string` keeps its signature but returns a `_s<idx>` suffixed name when `Streams > 1`.

This is the pivot that keeps the `Topology` interface untouched: `sinkTopology.Pipeline` already calls `n.IcebergTable("connect")`, so pointing it at a per-stream `BenchNames` is all a stream config needs.

- [ ] **Step 1: Write the failing tests**

Add to `benchmarking/aws/runner/topology_test.go`:

```go
func TestBenchNames_SingleStreamTableNamesUnchanged(t *testing.T) {
	// Streams 0 (zero value) and 1 must both yield the historical unsuffixed
	// name — this is what keeps the six existing scenarios untouched.
	for _, n := range []BenchNames{
		newBenchNames("sess-x", "iceberg"),
		newBenchNames("sess-x", "iceberg").WithStreams(1),
	} {
		if got := n.IcebergTable("connect"); got != "bench_sess_x_iceberg_connect" {
			t.Errorf("IcebergTable(connect) = %q, want unsuffixed bench_sess_x_iceberg_connect", got)
		}
		if got := n.IcebergTables("connect"); len(got) != 1 || got[0] != "bench_sess_x_iceberg_connect" {
			t.Errorf("IcebergTables(connect) = %v, want one unsuffixed name", got)
		}
	}
}

func TestBenchNames_MultiStreamTableNamesSuffixed(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg").WithStreams(2)
	if got := n.WithStream(0).IcebergTable("connect"); got != "bench_sess_x_iceberg_connect_s0" {
		t.Errorf("stream 0 table = %q", got)
	}
	if got := n.WithStream(1).IcebergTable("connect"); got != "bench_sess_x_iceberg_connect_s1" {
		t.Errorf("stream 1 table = %q", got)
	}
	want := []string{"bench_sess_x_iceberg_connect_s0", "bench_sess_x_iceberg_connect_s1"}
	got := n.IcebergTables("connect")
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("IcebergTables = %v, want %v", got, want)
	}
}

func TestBenchNames_SharedIdentifiersAreStreamIndependent(t *testing.T) {
	// Both streams join the same consumer group and read the same topic — that
	// is what splits the partitions 8/8 instead of doubling the work.
	n := newBenchNames("sess-x", "iceberg").WithStreams(2)
	if a, b := n.WithStream(0).ConsumerGroup("connect"), n.WithStream(1).ConsumerGroup("connect"); a != b {
		t.Errorf("consumer group must be shared across streams: %q vs %q", a, b)
	}
	if a, b := n.WithStream(0).SourceTopic(), n.WithStream(1).SourceTopic(); a != b {
		t.Errorf("source topic must be shared across streams: %q vs %q", a, b)
	}
}

func TestBenchNames_IcebergResetTablesIsUnionAcrossArms(t *testing.T) {
	n := newBenchNames("sess-x", "iceberg")
	// maxStreams 1: just the base table, exactly as before arms existed.
	got := n.IcebergResetTables("connect", 1)
	if len(got) != 1 || got[0] != "bench_sess_x_iceberg_connect" {
		t.Errorf("maxStreams=1 reset tables = %v, want [base]", got)
	}
	// maxStreams 2: base (for single-stream arms and for KC) plus both
	// per-stream tables, so one reset script serves every arm.
	got = n.IcebergResetTables("connect", 2)
	want := []string{
		"bench_sess_x_iceberg_connect",
		"bench_sess_x_iceberg_connect_s0",
		"bench_sess_x_iceberg_connect_s1",
	}
	if len(got) != 3 {
		t.Fatalf("maxStreams=2 reset tables = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reset table[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run 'TestBenchNames' -v`

Expected: FAIL — `WithStreams`, `WithStream`, `IcebergTables`, `IcebergResetTables` undefined.

- [ ] **Step 3: Write the implementation**

In `benchmarking/aws/runner/topology.go`, replace the `BenchNames` struct and add the new methods next to `IcebergTable`:

```go
type BenchNames struct {
	SessionID string
	Connector string
	// Streams is the arm's stream count. 0 and 1 both mean single-stream, in
	// which case Iceberg table names are unsuffixed exactly as they were
	// before matrix.arms existed. > 1 suffixes each stream's table with
	// _s<StreamIndex> so concurrent streams commit to independent tables.
	Streams int
	// StreamIndex selects which stream's names to render. Only meaningful
	// when Streams > 1.
	StreamIndex int
}

// WithStreams returns a copy scoped to an arm's stream count, resetting the
// stream index to 0.
func (n BenchNames) WithStreams(count int) BenchNames {
	n.Streams = count
	n.StreamIndex = 0
	return n
}

// WithStream returns a copy scoped to one stream of a multi-stream arm.
func (n BenchNames) WithStream(idx int) BenchNames {
	n.StreamIndex = idx
	return n
}

// icebergTableBase is the unsuffixed per-engine Glue table name. Glue/SQL
// identifiers can't contain '-', so the session id's dashes become underscores.
func (n BenchNames) icebergTableBase(engine string) string {
	safe := strings.ReplaceAll(n.SessionID, "-", "_")
	return fmt.Sprintf("bench_%s_%s_%s", safe, n.Connector, engine)
}

// IcebergTable is the Glue table this stream writes. Unsuffixed for
// single-stream arms; _s<StreamIndex> when the arm runs multiple streams.
func (n BenchNames) IcebergTable(engine string) string {
	base := n.icebergTableBase(engine)
	if n.Streams <= 1 {
		return base
	}
	return fmt.Sprintf("%s_s%d", base, n.StreamIndex)
}

// IcebergTables is every table this arm writes, in stream order. Throughput for
// a multi-stream arm is the summed committed-bytes growth across all of them.
func (n BenchNames) IcebergTables(engine string) []string {
	if n.Streams <= 1 {
		return []string{n.icebergTableBase(engine)}
	}
	out := make([]string, 0, n.Streams)
	for i := 0; i < n.Streams; i++ {
		out = append(out, n.WithStream(i).IcebergTable(engine))
	}
	return out
}

// IcebergResetTables is the union of tables any arm in the plan might write:
// the base name (used by single-stream arms and by Kafka Connect) plus every
// per-stream name up to maxStreams. The between-points reset drops and
// pre-creates all of them, so a single precomputed reset script serves every
// arm and each arm still starts from zero committed bytes.
func (n BenchNames) IcebergResetTables(engine string, maxStreams int) []string {
	out := []string{n.icebergTableBase(engine)}
	if maxStreams <= 1 {
		return out
	}
	for i := 0; i < maxStreams; i++ {
		out = append(out, n.WithStreams(maxStreams).WithStream(i).IcebergTable(engine))
	}
	return out
}
```

- [ ] **Step 4: Run the whole package to verify nothing regressed**

Run: `go test ./benchmarking/aws/runner/ -v -run 'TestBenchNames|TestTopology|TestSink'`

Expected: PASS. `TestBenchNames_SinkConventions` (pre-existing) must still pass unchanged — it asserts the unsuffixed name on a zero-value `BenchNames`.

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_test.go
git commit -m "feat(bench): stream-aware iceberg table names on BenchNames"
```

---

### Task 4: Key metric artifacts by point key instead of raw vCPU

**Files:**
- Modify: `benchmarking/aws/runner/topology.go` (`Topology` interface `MetricArtifact`, `MetricSidecarArgs`)
- Modify: `benchmarking/aws/runner/topology_source.go:71-77`, `:102-103`
- Modify: `benchmarking/aws/runner/topology_sink.go:143-156`
- Modify: `benchmarking/aws/runner/matrix.go:118-125` (sidecar args), `:212`, `:308-329` (`fetchBrokerSeriesForEngine`)
- Test: `benchmarking/aws/runner/topology_test.go`, existing `matrix_test.go` call sites

**Interfaces:**
- Consumes: nothing new.
- Produces: `MetricArtifact(engine, key string) string` (was `(engine string, vcpu int)`); `MetricSidecarArgs` gains `Key string`; `fetchBrokerSeriesForEngine(ctx, engine, key string)`.

This is a pure rename-and-rethread task: pass `strconv.Itoa(n)` at every call site so behaviour is unchanged. Task 7 swaps in the real point key.

- [ ] **Step 1: Write the failing test**

Add to `benchmarking/aws/runner/topology_test.go`:

```go
func TestMetricArtifact_KeyedByPointKey(t *testing.T) {
	// A bare vCPU key reproduces the historical filenames exactly.
	if got := (sourceTopology{}).MetricArtifact("connect", "4"); got != "redpanda-4-connect.txt" {
		t.Errorf("source MetricArtifact = %q, want redpanda-4-connect.txt", got)
	}
	if got := (sourceTopology{}).MetricArtifact("kafka_connect", "4"); got != "redpanda-4-kc.txt" {
		t.Errorf("source MetricArtifact(kc) = %q, want redpanda-4-kc.txt", got)
	}
	if got := (sinkTopology{}).MetricArtifact("connect", "2"); got != "iceberg-2-connect.txt" {
		t.Errorf("sink MetricArtifact = %q, want iceberg-2-connect.txt", got)
	}
	// An arm key flows straight through, giving each arm its own artifact.
	if got := (sinkTopology{}).MetricArtifact("connect", "2-b-2pipe-gmp4"); got != "iceberg-2-b-2pipe-gmp4-connect.txt" {
		t.Errorf("sink MetricArtifact(arm) = %q", got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./benchmarking/aws/runner/ -run TestMetricArtifact_KeyedByPointKey -v`

Expected: FAIL to compile — `MetricArtifact` takes an `int`.

- [ ] **Step 3: Change the signature and rethread every call site**

In `benchmarking/aws/runner/topology.go`:

```go
type MetricSidecarArgs struct {
	Engine string
	VCPU   int
	// Key identifies the sweep point in artifact names: the bare vCPU count
	// for arm-less scenarios, "<vcpu>-<armID>" when arms are in play. Falls
	// back to VCPU when empty so narrow unit tests can omit it.
	Key       string
	Bucket    string
	SessionID string
	Outs      map[string]string
	Names     BenchNames
}

// ArtifactKey is Key, or the bare vCPU count when Key was not set.
func (a MetricSidecarArgs) ArtifactKey() string {
	if a.Key != "" {
		return a.Key
	}
	return strconv.Itoa(a.VCPU)
}
```

Add `"strconv"` to that file's imports. Change the interface method:

```go
	// MetricArtifact is the per-engine, per-point metrics dump basename that
	// the bench script uploads and EngineSeries later parses. key is the
	// sweepPoint key: a bare vCPU count without arms, "<vcpu>-<armID>" with.
	MetricArtifact(engine, key string) string
```

In `topology_source.go`:

```go
func (sourceTopology) MetricArtifact(engine, key string) string {
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("redpanda-%s-%s.txt", key, suffix)
}
```

and at `:103`: `artifact := t.MetricArtifact(args.Engine, args.ArtifactKey())`.

In `topology_sink.go`:

```go
func (sinkTopology) MetricArtifact(engine, key string) string {
	suffix := engine
	if engine == "kafka_connect" {
		suffix = "kc"
	}
	return fmt.Sprintf("iceberg-%s-%s.txt", key, suffix)
}
```

and at `:152`: `artifact := t.MetricArtifact(args.Engine, args.ArtifactKey())`.

In `matrix.go`, at the sidecar construction (`:118-125`) add `Key: strconv.Itoa(n),` and change `fetchBrokerSeriesForEngine`:

```go
func (m *MatrixRunner) fetchBrokerSeriesForEngine(ctx context.Context, engine, key string) []TopicPoint {
	...
	key2 := fmt.Sprintf("runs/%s/%s", m.SessionID, m.Topology.MetricArtifact(engine, key))
	...
}
```

Rename the inner variable to avoid shadowing the parameter (e.g. `s3Key`). Update the call at `:212` to `m.fetchBrokerSeriesForEngine(ctx, engine, strconv.Itoa(n))`. Add `"strconv"` to `matrix.go` imports.

- [ ] **Step 4: Run the full package**

Run: `task test:unit -- benchmarking/aws/runner`

Expected: PASS. Any existing test that constructs `MetricSidecarArgs` without `Key` still works via `ArtifactKey()`'s fallback — for example `TestRenderBenchScript_RedpandaScraperWhenEndpointSet` must still see `RP=/tmp/redpanda-4-connect.txt`. Fix any compile errors in test files by adding the `Key` field or leaving it unset; do not change any expected string.

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/
git commit -m "refactor(bench): key metric artifacts by sweep-point key, not raw vCPU"
```

---

### Task 5: Sink metric sidecar sums every stream's table

**Files:**
- Modify: `benchmarking/aws/runner/topology_sink.go:151-183` (`MetricSidecar`)
- Test: `benchmarking/aws/runner/topology_test.go`

**Interfaces:**
- Consumes: `BenchNames.IcebergTables` (Task 3), `MetricSidecarArgs.ArtifactKey` (Task 4).
- Produces: no new Go symbols. The emitted shell still writes exactly two lines per frame (`total_files_size_bytes <n>`, `total_records <n>`), so `ParseIcebergSeries` is unchanged.

- [ ] **Step 1: Write the failing tests**

Add to `benchmarking/aws/runner/topology_test.go`:

```go
func TestSinkMetricSidecar_SingleTableShapeUnchanged(t *testing.T) {
	sc := sinkTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2",
		Bucket: "b", SessionID: "sess-x",
		Outs:  map[string]string{"aws_region": "us-east-2"},
		Names: newBenchNames("sess-x", "iceberg"),
	})
	if !strings.Contains(sc.Setup, "RP=/tmp/iceberg-2-connect.txt") {
		t.Errorf("artifact path missing:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "bench_sess_x_iceberg_connect") {
		t.Errorf("base table missing:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "total_files_size_bytes") || !strings.Contains(sc.Setup, "total_records") {
		t.Errorf("sidecar must still emit both metric lines:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "s3://b/runs/sess-x/iceberg-2-connect.txt") {
		t.Errorf("upload target wrong:\n%s", sc.Upload)
	}
}

func TestSinkMetricSidecar_SumsAcrossStreamTables(t *testing.T) {
	sc := sinkTopology{}.MetricSidecar(MetricSidecarArgs{
		Engine: "connect", VCPU: 2, Key: "2-b-2pipe-gmp4",
		Bucket: "b", SessionID: "sess-x",
		Outs:  map[string]string{"aws_region": "us-east-2"},
		Names: newBenchNames("sess-x", "iceberg").WithStreams(2),
	})
	for _, want := range []string{
		"RP=/tmp/iceberg-2-b-2pipe-gmp4-connect.txt",
		"bench_sess_x_iceberg_connect_s0",
		"bench_sess_x_iceberg_connect_s1",
	} {
		if !strings.Contains(sc.Setup, want) {
			t.Errorf("sidecar missing %q:\n%s", want, sc.Setup)
		}
	}
	// The poller must accumulate, not overwrite, or a 2-stream arm reports
	// only one stream's bytes and the whole A/B is wrong.
	if !strings.Contains(sc.Setup, "SIZE=$((SIZE + ") || !strings.Contains(sc.Setup, "RECS=$((RECS + ") {
		t.Errorf("sidecar must accumulate across tables:\n%s", sc.Setup)
	}
	// Still exactly two emitted metric lines per frame, so ParseIcebergSeries
	// needs no change.
	if got := strings.Count(sc.Setup, `echo "total_files_size_bytes`); got != 1 {
		t.Errorf("expected exactly one size emission per frame, got %d:\n%s", got, sc.Setup)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run TestSinkMetricSidecar -v`

Expected: FAIL — the current sidecar polls one table and assigns rather than accumulates, so `SIZE=$((SIZE + ` is absent.

- [ ] **Step 3: Write the implementation**

In `benchmarking/aws/runner/topology_sink.go`, replace the body of `MetricSidecar` (keeping the method signature) with:

```go
func (t sinkTopology) MetricSidecar(args MetricSidecarArgs) MetricSidecar {
	artifact := t.MetricArtifact(args.Engine, args.ArtifactKey())
	sp, _ := sinkSpecFor(args.Names.Connector) // ok ignored: Validate guarantees the sinkSpec exists
	region := args.Outs["aws_region"]
	db := sp.Namespace
	// A multi-stream arm writes one table per stream; the arm's throughput is
	// the summed committed-bytes growth across all of them. Single-stream arms
	// yield a one-element list, so the shell shape is identical either way.
	tables := args.Names.IcebergTables(args.Engine)
	setup := fmt.Sprintf(`RP=/tmp/%s
: > "$RP"
(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      SIZE=0
      RECS=0
      for T in %s; do
        META=$(aws glue get-table --region %q --database-name %q --name "$T" \
                --query 'Table.Parameters.metadata_location' --output text 2>/dev/null || echo "")
        if [ -n "$META" ] && [ "$META" != "None" ]; then
          SNAP=$(aws s3 cp "$META" - 2>/dev/null || echo '{}')
          S=$(echo "$SNAP" | jq -r '[.snapshots[]?."summary"."total-files-size" // "0" | tonumber] | last // 0' 2>/dev/null || echo 0)
          R=$(echo "$SNAP" | jq -r '[.snapshots[]?."summary"."total-records" // "0" | tonumber] | last // 0' 2>/dev/null || echo 0)
          SIZE=$((SIZE + ${S:-0}))
          RECS=$((RECS + ${R:-0}))
        fi
      done
      echo "total_files_size_bytes ${SIZE:-0}"
      echo "total_records ${RECS:-0}"
    } >> "$RP"
    sleep 10
  done
) &
RP_SCRAPER=$!`, artifact, strings.Join(tables, " "), region, db)
	upload := fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/%s" >/dev/null`,
		args.Bucket, args.SessionID, artifact)
	return MetricSidecar{Setup: setup, Upload: upload}
}
```

Note the `%%s` in `date +%%s` — it must survive as a literal `%s` in the shell.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./benchmarking/aws/runner/ -run 'TestSinkMetricSidecar|TestParseIcebergSeries' -v`

Expected: PASS, including the pre-existing `icebergmetrics_test.go` cases (the emitted line format is unchanged).

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/topology_sink.go benchmarking/aws/runner/topology_test.go
git commit -m "feat(bench): sink metric sidecar sums committed bytes across stream tables"
```

---

### Task 6: Sink reset pre-creates the union of every arm's tables

**Files:**
- Modify: `benchmarking/aws/runner/topology_sink.go:111-137` (`ResetScript`)
- Test: `benchmarking/aws/runner/topology_test.go`

**Interfaces:**
- Consumes: `BenchNames.IcebergResetTables` (Task 3), `BenchNames.Streams`.
- Produces: no new symbols. `ResetScript` keeps its `(s, outs, n)` signature; the caller passes a `BenchNames` whose `Streams` is the plan's max (Task 8), and `ResetScript` reads it.

Rationale for the union rather than a per-arm reset: `ResetScript` is rendered once before the sweep and passed to `MatrixRunner.Run` as a single string. Dropping and pre-creating the base table plus `_s0.._sN-1` before every point means each arm finds its own tables at zero committed bytes, unused tables sit empty and cost nothing, and no signature anywhere has to become point-aware.

- [ ] **Step 1: Write the failing tests**

Add to `benchmarking/aws/runner/topology_test.go`:

```go
func TestSinkResetScript_SingleStreamUnchanged(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink}
	outs := map[string]string{
		"aws_region": "us-east-2", "redpanda_broker_endpoints": "b:9092",
		"glue_rest_uri": "https://glue", "warehouse_account_id": "1234",
		"warehouse_s3_uri": "s3://wh",
	}
	got, err := sinkTopology{}.ResetScript(s, outs, newBenchNames("sess-x", "iceberg"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"bench_sess_x_iceberg_connect",
		"bench_sess_x_iceberg_kafka_connect",
		"kafka-consumer-groups.sh",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("reset missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "_s0") {
		t.Errorf("single-stream reset must not mention per-stream tables:\n%s", got)
	}
	// One delete + one pre-create per engine.
	if n := strings.Count(got, "aws glue delete-table"); n != 2 {
		t.Errorf("expected 2 delete-table calls (one per engine), got %d", n)
	}
}

func TestSinkResetScript_CreatesUnionForMultiStreamPlan(t *testing.T) {
	s := &Scenario{Connector: "iceberg", Direction: DirectionSink}
	outs := map[string]string{
		"aws_region": "us-east-2", "redpanda_broker_endpoints": "b:9092",
		"glue_rest_uri": "https://glue", "warehouse_account_id": "1234",
		"warehouse_s3_uri": "s3://wh",
	}
	// Streams here is the plan max, so the reset serves every arm.
	got, err := sinkTopology{}.ResetScript(s, outs, newBenchNames("sess-x", "iceberg").WithStreams(2))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"bench_sess_x_iceberg_connect",     // base: used by the single-stream arms
		"bench_sess_x_iceberg_connect_s0",  // arm B stream 0
		"bench_sess_x_iceberg_connect_s1",  // arm B stream 1
		"bench_sess_x_iceberg_kafka_connect",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("reset missing %q:\n%s", want, got)
		}
	}
	// 3 connect tables + 3 kafka_connect tables, each dropped and pre-created.
	if n := strings.Count(got, "aws glue delete-table"); n != 6 {
		t.Errorf("expected 6 delete-table calls, got %d:\n%s", n, got)
	}
	if n := strings.Count(got, "/opt/bench/iceberg-tablegen"); n != 6 {
		t.Errorf("expected 6 tablegen pre-creates, got %d:\n%s", n, got)
	}
	// Each pre-create is wrapped in a bounded retry that still fails loud.
	if n := strings.Count(got, "for attempt in 1 2 3; do"); n != 6 {
		t.Errorf("expected each tablegen wrapped in a retry loop, got %d:\n%s", n, got)
	}
	if !strings.Contains(got, "after 3 attempts") {
		t.Errorf("retry must fail loud after 3 attempts:\n%s", got)
	}
	// The consumer group is shared by both streams, so it is reset once per
	// engine, not once per table.
	if n := strings.Count(got, "kafka-consumer-groups.sh"); n != 2 {
		t.Errorf("expected 2 consumer-group resets (one per engine), got %d:\n%s", n, got)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run TestSinkResetScript -v`

Expected: FAIL — the multi-stream case finds 2 delete-table calls, not 6.

- [ ] **Step 3: Write the implementation**

In `benchmarking/aws/runner/topology_sink.go`, replace the `for _, eng := range ...` loop body inside `ResetScript` with:

```go
	for _, eng := range []string{"connect", "kafka_connect"} {
		// Reset the union of every arm's tables: the base name plus each
		// per-stream name up to the plan's max stream count. n.Streams carries
		// that max (see planMaxStreams), which lets this one precomputed
		// script serve every arm — each arm's own tables start at zero
		// committed bytes and the extras sit empty.
		for _, table := range n.IcebergResetTables(eng, n.Streams) {
			// Drop the table so total-files-size restarts at 0.
			w(`aws glue delete-table --region %q --database-name %q --name %q 2>/dev/null || true`,
				region, db, table)
			// Pre-create with an explicit location: the Glue REST catalog
			// requires one on create and the KC Tabular sink does not supply it.
			//
			// Retried, because this script runs under `set -euo pipefail` and
			// iceberg-tablegen exits non-zero on transient Glue/IAM errors (it
			// already treats "already exists" as success). The table union turns
			// one unguarded call per engine into N, and a single throttled call
			// would otherwise abort the whole sweep at reset time. Three
			// attempts, then fail LOUD: a missing table must never be silently
			// tolerated, because the stream that needed it would commit nothing
			// and deflate its arm's throughput instead of erroring.
			//
			// `if cmd; then` (not `cmd && break`) because the `if` condition is
			// explicitly exempt from `-e`, making the retry's semantics
			// unambiguous.
			w(`for attempt in 1 2 3; do`)
			w(`  if /opt/bench/iceberg-tablegen --catalog-uri=%s --warehouse=%s --region=%s --namespace=%s --table=%s --location=%s; then break; fi`,
				catalogURI, warehouse, region, db, table, fmt.Sprintf("%s/%s/%s", whBase, db, table))
			w(`  if [ "$attempt" = 3 ]; then echo "iceberg-tablegen failed for %s after 3 attempts" >&2; exit 1; fi`, table)
			w(`  sleep 5`)
			w(`done`)
		}
		// Reset the per-engine consumer group to re-read the whole topic. Both
		// streams of a multi-stream arm share this group — that is what splits
		// the partitions between them instead of doubling the work.
		w(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %q --group %q --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || true`,
			brokers, n.ConsumerGroup(eng))
	}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `task test:unit -- benchmarking/aws/runner`

Expected: PASS across the package.

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/topology_sink.go benchmarking/aws/runner/topology_test.go
git commit -m "feat(bench): sink reset pre-creates union of per-arm iceberg tables"
```

---

### Task 7: Bench script launches `run` or `streams` with an explicit GOMAXPROCS

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go:347-370` (`benchScriptArgs`), `:380-457` (`renderBenchScript`)
- Test: `benchmarking/aws/runner/matrix_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `benchScriptArgs` gains `GOMAXPROCS int`, `Streams int`, `Key string`, `RootConfigPath string`, `StreamsDir string`. Zero values preserve today's behaviour (`GOMAXPROCS` falls back to `VCPU`, `Key` to `strconv.Itoa(VCPU)`, `Streams <= 1` uses `run ConfigPath`).

- [ ] **Step 1: Write the failing tests**

Add to `benchmarking/aws/runner/matrix_test.go`:

```go
func TestRenderBenchScript_DefaultsGOMAXPROCSToVCPU(t *testing.T) {
	// Zero-value GOMAXPROCS/Key/Streams must reproduce the pre-arms script.
	got := renderBenchScript(benchScriptArgs{
		VCPU: 4, MemLimitGiB: 8, WarmupSec: 0, DurationSec: 900,
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "b", SessionID: "s",
	})
	require.Contains(t, got, "GOMAXPROCS=4")
	require.Contains(t, got, "taskset -c 2-5")
	require.Contains(t, got, "/opt/bench/redpanda-connect run /opt/bench/config.yaml")
	require.Contains(t, got, "/tmp/bench-4.log")
	require.Contains(t, got, "s3://b/runs/s/sweep-4.log")
	require.NotContains(t, got, "streams -o")
}

func TestRenderBenchScript_OversubscribesGOMAXPROCSWithoutWideningTaskset(t *testing.T) {
	got := renderBenchScript(benchScriptArgs{
		VCPU: 2, GOMAXPROCS: 4, Streams: 1, Key: "2-a1-1pipe-gmp4",
		MemLimitGiB: 4, DurationSec: 900,
		ConfigPath: "/opt/bench/cfg/2-a1-1pipe-gmp4/config.yaml",
		BinaryPath: "/opt/bench/redpanda-connect",
		Bucket:     "b", SessionID: "s",
	})
	require.Contains(t, got, "GOMAXPROCS=4")
	require.Contains(t, got, "taskset -c 2-3", "the core pin must still follow VCPU, not GOMAXPROCS")
	require.Contains(t, got, "GOMEMLIMIT=4GiB", "memory stays vCPU-derived so arms are memory-fair")
	require.Contains(t, got, "/tmp/bench-2-a1-1pipe-gmp4.log")
	require.Contains(t, got, "s3://b/runs/s/sweep-2-a1-1pipe-gmp4.log")
	require.Contains(t, got, "s3://b/runs/s/prom-2-a1-1pipe-gmp4.txt")
}

func TestRenderBenchScript_StreamsModeLaunch(t *testing.T) {
	got := renderBenchScript(benchScriptArgs{
		VCPU: 2, GOMAXPROCS: 4, Streams: 2, Key: "2-b-2pipe-gmp4",
		MemLimitGiB: 4, DurationSec: 900,
		RootConfigPath: "/opt/bench/cfg/2-b-2pipe-gmp4/root.yaml",
		StreamsDir:     "/opt/bench/cfg/2-b-2pipe-gmp4/streams",
		BinaryPath:     "/opt/bench/redpanda-connect",
		Bucket:         "b", SessionID: "s",
	})
	require.Contains(t, got,
		"/opt/bench/redpanda-connect streams -o /opt/bench/cfg/2-b-2pipe-gmp4/root.yaml /opt/bench/cfg/2-b-2pipe-gmp4/streams")
	require.NotContains(t, got, "redpanda-connect run ")
	require.Contains(t, got, "GOMAXPROCS=4")
	require.Contains(t, got, "taskset -c 2-3")
	require.Contains(t, got, "/tmp/bench-2-b-2pipe-gmp4.log")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run TestRenderBenchScript -v`

Expected: FAIL to compile — `GOMAXPROCS`, `Streams`, `Key`, `RootConfigPath`, `StreamsDir` are not fields of `benchScriptArgs`.

- [ ] **Step 3: Write the implementation**

In `benchmarking/aws/runner/matrix.go`, add to `benchScriptArgs`:

```go
	// GOMAXPROCS is the Go runtime's P count. 0 means "same as VCPU", which is
	// the pre-arms default. An arm may deliberately set it above VCPU: the
	// taskset core pin below always follows VCPU, so oversubscribing only
	// changes how many goroutines the runtime will schedule onto those cores.
	GOMAXPROCS int
	// Streams > 1 launches `redpanda-connect streams -o <RootConfigPath>
	// <StreamsDir>` instead of `run <ConfigPath>`.
	Streams        int
	RootConfigPath string
	StreamsDir     string
	// Key names this point's artifacts. Empty means the bare vCPU count.
	Key string
```

Add a helper next to the struct:

```go
// artifactKey names this point's log and metric files: the bare vCPU count for
// arm-less scenarios (unchanged from before matrix.arms), "<vcpu>-<armID>" with
// arms.
func (a benchScriptArgs) artifactKey() string {
	if a.Key != "" {
		return a.Key
	}
	return strconv.Itoa(a.VCPU)
}

// gomaxprocs is the runtime P count for this point, defaulting to the pinned
// core count.
func (a benchScriptArgs) gomaxprocs() int {
	if a.GOMAXPROCS > 0 {
		return a.GOMAXPROCS
	}
	return a.VCPU
}

// launchCmd is the engine invocation: streams mode when the point runs more
// than one pipeline in the process, single-config run mode otherwise.
func (a benchScriptArgs) launchCmd() string {
	if a.Streams > 1 {
		return fmt.Sprintf("%s streams -o %s %s", a.BinaryPath, a.RootConfigPath, a.StreamsDir)
	}
	return fmt.Sprintf("%s run %s", a.BinaryPath, a.ConfigPath)
}
```

In `renderBenchScript`, replace the header/log/launch lines:

```go
	cpusetHi := 1 + a.VCPU // inclusive
	key := a.artifactKey()
	totalSec := a.WarmupSec + a.DurationSec
	lines := []string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting bench: %d vCPU, GOMAXPROCS %d, %d streams, %d GiB, warmup %ds, window %ds"`,
			a.VCPU, a.gomaxprocs(), max(a.Streams, 1), a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`LOG=/tmp/bench-%s.log`, key),
		fmt.Sprintf(`PROM=/tmp/prom-%s.txt`, key),
		`: > "$LOG"`,
		`: > "$PROM"`,
		// chrt removed for scheduler parity with KC (it deadlocked the JVM
		// under single-core taskset; see traps reference in the
		// bench-framework Claude skill). taskset alone gives us CPU
		// isolation; SCHED_OTHER is what KC uses.
		//
		// The core pin follows VCPU while GOMAXPROCS is independent: an arm can
		// oversubscribe the runtime on a fixed core allocation. GOMEMLIMIT is
		// vCPU-derived by the caller, so it is constant across an A/B's arms.
		fmt.Sprintf(`taskset -c 2-%d env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s >"$LOG" 2>&1 &`,
			cpusetHi, a.gomaxprocs(), a.MemLimitGiB, a.launchCmd()),
		`PID=$!`,
```

Replace the two S3 upload lines at the end with `key`-based paths:

```go
		fmt.Sprintf(`aws s3 cp "$LOG" "s3://%s/runs/%s/sweep-%s.log" >/dev/null`,
			a.Bucket, a.SessionID, key),
		fmt.Sprintf(`aws s3 cp "$PROM" "s3://%s/runs/%s/prom-%s.txt" >/dev/null`,
			a.Bucket, a.SessionID, key),
```

Add `"strconv"` to the imports if Task 4 did not already.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./benchmarking/aws/runner/ -run TestRenderBenchScript -v`

Expected: PASS, including the pre-existing `TestRenderBenchScript_EmbedsBucketAndSession` with its `/tmp/bench-4.log` and `taskset -c 2-5` assertions — that is the parity proof for this task.

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go
git commit -m "feat(bench): bench script supports GOMAXPROCS override and streams-mode launch"
```

---

### Task 8: `MatrixRunner.Run` sweeps plan points

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go:23-76` (`MatrixRunner`, `SweepPoint`), `:81-262` (`Run`)
- Modify: `benchmarking/aws/runner/main.go:305` (call site)
- Test: `benchmarking/aws/runner/matrix_test.go`

**Interfaces:**
- Consumes: `sweepPoint`, `buildSweepPlan` (Task 2); `benchScriptArgs` fields (Task 7).
- Produces:
  - `MatrixRunner.Run(ctx, plan []sweepPoint, memLimitPerVCPU int, warmup, duration time.Duration, resetScript, workloadScript string)` — the first argument changes from `[]int`.
  - `MatrixRunner.ConfigPaths map[string]pointConfigPaths` — nil means "use `ConfigPath` for every point" (the pre-arms path).
  - `type pointConfigPaths struct { Single, Root, Dir string }`
  - `SweepPoint` gains `ArmID string` and `GOMAXPROCS int`.

- [ ] **Step 1: Write the failing tests**

First, teach the existing test double to record what it was asked to run. In `benchmarking/aws/runner/ssm.go:98`, add a field to `FakeSSM` and append in `Run` (the parameter is currently discarded as `_`):

```go
type FakeSSM struct {
	Transcripts map[string][]string // instanceID → lines to emit on Run
	Errs        map[string]error
	// Scripts records every script submitted, in order, so tests can assert on
	// what the runner actually asked the host to execute.
	Scripts []string
}

func (f *FakeSSM) Run(_ context.Context, instanceID, script string, onLine func(string)) error {
	f.Scripts = append(f.Scripts, script)
	for _, line := range f.Transcripts[instanceID] {
		if onLine != nil {
			onLine(line)
		}
	}
	return f.Errs[instanceID]
}
```

Then add to `benchmarking/aws/runner/matrix_test.go`. `makeLog`, `FakeSSM` and `FakeLogFetcher` are already available in this package — the arm-keyed log filenames are what the fetcher must be primed with.

```go
func TestMatrixRunner_RunSweepsEveryArm(t *testing.T) {
	// Two arms at one vCPU point must produce two SweepPoints carrying their
	// arm id and GOMAXPROCS, each launched from its own config paths.
	const sessionID = "sess-x"
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-2-a0.log", sessionID): makeLog(30, 60),
			fmt.Sprintf("runs/%s/sweep-2-b.log", sessionID):  makeLog(30, 90),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM: ssm, LogFetcher: fetcher, RunnerInstance: "i-runner",
		Bucket: "b", SessionID: sessionID,
		ConfigPaths: map[string]pointConfigPaths{
			"2-a0": {Single: "/opt/bench/cfg/2-a0/config.yaml"},
			"2-b":  {Root: "/opt/bench/cfg/2-b/root.yaml", Dir: "/opt/bench/cfg/2-b/streams"},
		},
	}
	plan := []sweepPoint{
		{VCPU: 2, ArmID: "a0", GOMAXPROCS: 2, Streams: 1},
		{VCPU: 2, ArmID: "b", GOMAXPROCS: 4, Streams: 2},
	}
	points, err := mr.Run(context.Background(), plan, 2, 0, 30*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 2)
	require.Equal(t, "a0", points[0].ArmID)
	require.Equal(t, 2, points[0].GOMAXPROCS)
	require.Equal(t, "b", points[1].ArmID)
	require.Equal(t, 4, points[1].GOMAXPROCS)
	require.Equal(t, 2, points[0].VCPU, "every arm shares the vCPU pin")
	require.Equal(t, 2, points[1].VCPU)

	// Each arm's script must reference that arm's own config paths and its own
	// GOMAXPROCS.
	scripts := strings.Join(ssm.Scripts, "\n---\n")
	require.Contains(t, scripts, "run /opt/bench/cfg/2-a0/config.yaml")
	require.Contains(t, scripts, "streams -o /opt/bench/cfg/2-b/root.yaml /opt/bench/cfg/2-b/streams")
	require.Contains(t, scripts, "GOMAXPROCS=2")
	require.Contains(t, scripts, "GOMAXPROCS=4")
}

func TestMatrixRunner_ArmlessPlanUsesLegacyConfigPath(t *testing.T) {
	// nil ConfigPaths → every point launches the single staged config at the
	// historical path, exactly as the six existing scenarios do.
	const sessionID = "sess-y"
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): makeLog(30, 50),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": {"bench point complete"}}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM: ssm, LogFetcher: fetcher, RunnerInstance: "i-runner",
		Bucket: "b", SessionID: sessionID,
		ConfigPath: "/opt/bench/config.yaml",
	}
	points, err := mr.Run(context.Background(), []sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}, 2, 0, 30*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Empty(t, points[0].ArmID)
	scripts := strings.Join(ssm.Scripts, "\n")
	require.Contains(t, scripts, "run /opt/bench/config.yaml")
	require.Contains(t, scripts, "s3://b/runs/sess-y/sweep-1.log",
		"arm-less artifact keys stay bare-vCPU")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run TestMatrixRunner -v`

Expected: FAIL to compile — `Run` takes `[]int`; `ConfigPaths`, `pointConfigPaths`, `SweepPoint.ArmID` undefined.

- [ ] **Step 3: Write the implementation**

In `benchmarking/aws/runner/matrix.go`:

```go
// pointConfigPaths locates one sweep point's launch config(s) on the runner
// host. Single is set for single-pipeline points; Root and Dir are set for
// streams-mode points.
type pointConfigPaths struct {
	Single string
	Root   string
	Dir    string
}
```

Add to `MatrixRunner`:

```go
	// ConfigPaths maps sweepPoint.Key() to that point's launch config paths on
	// the runner host. Nil (the arm-less case) means every point launches
	// ConfigPath, which keeps existing scenarios on the historical
	// /opt/bench/config.yaml staging path.
	ConfigPaths map[string]pointConfigPaths
```

Add to `SweepPoint`:

```go
	// ArmID is "" for arm-less sweeps; the matrix.arms id otherwise.
	ArmID string
	// GOMAXPROCS is the runtime P count measured at this point. Equal to VCPU
	// unless an arm oversubscribed it.
	GOMAXPROCS int
```

Change `Run`'s signature and loop head:

```go
func (m *MatrixRunner) Run(
	ctx context.Context,
	plan []sweepPoint,
	memLimitPerVCPU int,
	warmup, duration time.Duration,
	resetScript string,
	workloadScript string,
) ([]SweepPoint, error) {
	engines := m.Engines
	if len(engines) == 0 {
		engines = []string{"connect"}
	}
	out := make([]SweepPoint, 0, len(plan)*len(engines))
	for _, pt := range plan {
		n := pt.VCPU
		key := pt.Key()
		for _, engine := range engines {
			if pt.ArmID == "" {
				fmt.Printf("=== sweep point: %d vCPU, engine=%s (warmup %s, window %s) ===\n", n, engine, warmup, duration)
			} else {
				fmt.Printf("=== sweep point: %d vCPU, arm=%s (GOMAXPROCS %d, %d streams), engine=%s (warmup %s, window %s) ===\n",
					n, pt.ArmID, pt.GOMAXPROCS, pt.Streams, engine, warmup, duration)
			}
			...
```

Inside the loop, make these substitutions:

- Sidecar args: `VCPU: n, Key: key,` **and `Names: m.Names.WithStreams(pt.Streams)`.**
  The `Names` scoping is load-bearing and easy to miss: `sinkTopology.MetricSidecar`
  derives the tables it polls from `args.Names.IcebergTables(args.Engine)`, so
  passing the base `m.Names` (`Streams == 0`) would poll only the unsuffixed
  table. For a 2-stream arm the streams write `_s0`/`_s1` while the unsuffixed
  table — which the reset union does create — is never written, so the arm would
  report ~0 MB/s with no error anywhere, and the A/B would read as "2 pipelines
  are far worse" rather than as a broken measurement. Add a test that runs a
  2-stream sink plan and asserts the submitted script references both per-stream
  table names.
  (`fetchBrokerSeriesForEngine`'s `MetricInputs{Names: m.Names}` needs no
  scoping — the sink's `EngineSeries` is `ParseIcebergSeries(in.Body)`, which
  ignores `Names`.)
- The `connect` branch of the script switch:

```go
			case "connect":
				cfg := m.configPathsFor(key)
				script = renderBenchScript(benchScriptArgs{
					VCPU:                     n,
					GOMAXPROCS:               pt.GOMAXPROCS,
					Streams:                  pt.Streams,
					Key:                      key,
					MemLimitGiB:              memLimitPerVCPU * n,
					WarmupSec:                int(warmup.Seconds()),
					DurationSec:              int(duration.Seconds()),
					ConfigPath:               cfg.Single,
					RootConfigPath:           cfg.Root,
					StreamsDir:               cfg.Dir,
					BinaryPath:               m.BinaryPath,
					Bucket:                   m.Bucket,
					SessionID:                m.SessionID,
					RedpandaMetricsEndpoint:  m.RedpandaMetricsEndpoint,
					RedpandaMetricsEndpoints: m.RedpandaMetricsEndpoints,
					ScrapeSetup:              sidecar.Setup,
					ScrapeUpload:             sidecar.Upload,
				})
```

- The `kafka_connect` branch keeps `VCPU: n` and `MemLimitGiB: memLimitPerVCPU * n`; its per-vCPU connector name stays `fmt.Sprintf("%s_v%d", m.KCConnectorName, n)` (arms are Connect-only, so no collision is possible).
- `m.fetchLog(ctx, key)` and `m.fetchProm(ctx, key)` — change both helpers to take a `key string` and format `runs/%s/sweep-%s.log` / `runs/%s/prom-%s.txt`.
- `brokerSeries := m.fetchBrokerSeriesForEngine(ctx, engine, key)`.
- The appended point:

```go
			out = append(out, SweepPoint{
				VCPU:         n,
				ArmID:        pt.ArmID,
				GOMAXPROCS:   pt.GOMAXPROCS,
				Engine:       engine,
				Samples:      samples,
				Summary:      summary,
				Anomalies:    anomalies,
				Prom:         promPts,
				BrokerSeries: brokerSeries,
			})
```

- The early-abort guard's `if n == cpuPoints[0]` becomes
  `if pt.Key() == plan[0].Key()`. Do NOT use `len(out) == 1`: that fires only
  once per sweep, whereas the original fired for **every engine** at the first
  vCPU point. Since the existing scenarios sweep both engines, `len(out) == 1`
  would stop checking Kafka Connect's first point, so a KC misconfiguration
  producing zero throughput would burn the whole 2-3h sweep instead of aborting
  immediately. Keying off the first plan point preserves the original semantics
  exactly: it fires once per engine at the first point, arms or not.
  `plan[0]` is safe to index — the loop body only runs when the plan is
  non-empty.

Add the resolver:

```go
// configPathsFor returns the launch config paths for a point key, falling back
// to the single staged ConfigPath when the scenario declares no arms.
func (m *MatrixRunner) configPathsFor(key string) pointConfigPaths {
	if cfg, ok := m.ConfigPaths[key]; ok {
		return cfg
	}
	return pointConfigPaths{Single: m.ConfigPath}
}
```

In `benchmarking/aws/runner/main.go`, replace line 305:

```go
	plan := buildSweepPlan(s)
	points, err := mr.Run(ctx, plan, s.Matrix.GoMemLimitPerVCPU, warmup, duration, reset, workload)
```

- [ ] **Step 4: Run the full package**

Run: `task test:unit -- benchmarking/aws/runner`

Expected: PASS. Existing `MatrixRunner` tests need their first argument rewritten from `[]int{1}` to `[]sweepPoint{{VCPU: 1, GOMAXPROCS: 1, Streams: 1}}`; do that mechanically and change no assertions.

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go benchmarking/aws/runner/main.go
git commit -m "feat(bench): MatrixRunner sweeps plan points instead of raw cpu_points"
```

---

### Task 9: Per-arm config rendering and staging

**Files:**
- Modify: `benchmarking/aws/runner/main.go:546-589` (`renderPipelineConfig`), `:707-747` (`stageArtefacts`), and the `runBench` wiring around `:270-305`
- Test: `benchmarking/aws/runner/scripts_test.go` (or a new `configrender_test.go`)

**Interfaces:**
- Consumes: `sweepPoint` (Task 2), `BenchNames.WithStreams`/`WithStream` (Task 3), `pointConfigPaths` (Task 8).
- Produces:
  - `type renderedPointConfigs struct { Key string; Single string; Root string; Streams []string }` (local temp file paths)
  - `func renderPointConfigs(s *Scenario, outs map[string]string, topo Topology, names BenchNames, p sweepPoint) (renderedPointConfigs, error)`
  - `func runnerConfigPaths(sets []renderedPointConfigs) map[string]pointConfigPaths`
  - `stageArtefacts(ctx, opts, outs, binPath string, sets []renderedPointConfigs, legacy bool) error` — signature change from `cfgPath string`. `legacy` is `len(s.Matrix.Arms) == 0` and selects the historical single-`stage/config.yaml` staging path.

- [ ] **Step 1: Write the failing tests**

Create `benchmarking/aws/runner/configrender_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func icebergArmScenario(t *testing.T) (*Scenario, map[string]string, Topology) {
	t.Helper()
	s, err := LoadScenario("testdata/valid-iceberg-arms.yaml")
	require.NoError(t, err)
	outs := map[string]string{
		"redpanda_broker_endpoints": "b1:9092",
		"glue_rest_uri":             "https://glue.example",
		"warehouse_account_id":      "1234",
		"aws_region":                "us-east-2",
		"s3_bucket":                 "wh-bucket",
		"warehouse_s3_uri":          "s3://wh-bucket/wh",
	}
	topo, err := topologyFor(s.Direction)
	require.NoError(t, err)
	return s, outs, topo
}

func readYAML(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &m))
	return m
}

func TestRenderPointConfigs_SingleStreamArm(t *testing.T) {
	s, outs, topo := icebergArmScenario(t)
	names := newBenchNames("sess-x", "iceberg")
	plan := buildSweepPlan(s)

	got, err := renderPointConfigs(s, outs, topo, names, plan[1]) // a1-1pipe-gmp4
	require.NoError(t, err)
	require.Equal(t, "2-a1-1pipe-gmp4", got.Key)
	require.NotEmpty(t, got.Single)
	require.Empty(t, got.Root)
	require.Empty(t, got.Streams)

	cfg := readYAML(t, got.Single)
	// One config carries everything, exactly as the pre-arms renderer produced.
	for _, k := range []string{"http", "redpanda", "input", "output", "logger", "metrics", "buffer"} {
		require.Contains(t, cfg, k, "single-stream config must keep section %q", k)
	}
	ice := cfg["output"].(map[string]any)["iceberg"].(map[string]any)
	require.Equal(t, "bench_sess_x_iceberg_connect", ice["table"], "single-stream arm keeps the unsuffixed table")
	require.Equal(t, 16, ice["max_in_flight"], "arm a1 inherits the scenario's max_in_flight")
}

func TestRenderPointConfigs_StreamsModeSplitsRootAndStreams(t *testing.T) {
	s, outs, topo := icebergArmScenario(t)
	names := newBenchNames("sess-x", "iceberg")
	plan := buildSweepPlan(s)

	got, err := renderPointConfigs(s, outs, topo, names, plan[2]) // b-2pipe-gmp4
	require.NoError(t, err)
	require.Equal(t, "2-b-2pipe-gmp4", got.Key)
	require.Empty(t, got.Single)
	require.NotEmpty(t, got.Root)
	require.Len(t, got.Streams, 2)

	// The root config carries observability and service-wide fields ONLY —
	// benthos streams mode rejects input/output there.
	root := readYAML(t, got.Root)
	for _, k := range []string{"http", "logger", "metrics"} {
		require.Contains(t, root, k)
	}
	for _, k := range []string{"input", "output", "buffer"} {
		require.NotContains(t, root, k, "root config must not carry stream field %q", k)
	}

	// Each stream config carries only its own pipeline.
	for i, path := range got.Streams {
		sc := readYAML(t, path)
		for _, k := range []string{"input", "output", "buffer"} {
			require.Contains(t, sc, k, "stream %d missing %q", i, k)
		}
		for _, k := range []string{"http", "logger", "metrics"} {
			require.NotContains(t, sc, k, "stream %d must not carry root field %q", i, k)
		}
		ice := sc["output"].(map[string]any)["iceberg"].(map[string]any)
		require.Equal(t, 8, ice["max_in_flight"], "arm B's override must reach every stream")
		buf := sc["buffer"].(map[string]any)["memory"].(map[string]any)
		require.Equal(t, 262144000, buf["limit"], "arm B halves each stream's buffer")
	}

	// Distinct tables per stream, shared topic and consumer group.
	s0 := readYAML(t, got.Streams[0])
	s1 := readYAML(t, got.Streams[1])
	t0 := s0["output"].(map[string]any)["iceberg"].(map[string]any)["table"]
	t1 := s1["output"].(map[string]any)["iceberg"].(map[string]any)["table"]
	require.Equal(t, "bench_sess_x_iceberg_connect_s0", t0)
	require.Equal(t, "bench_sess_x_iceberg_connect_s1", t1)

	in0 := s0["input"].(map[string]any)["redpanda"].(map[string]any)
	in1 := s1["input"].(map[string]any)["redpanda"].(map[string]any)
	require.Equal(t, in0["consumer_group"], in1["consumer_group"],
		"both streams share the group so the 16 partitions split, not double")
	require.Equal(t, in0["topics"], in1["topics"])

	// Placeholders must be resolved, not left as ${...}.
	require.NotContains(t, t0, "${")
	require.Equal(t, "https://glue.example",
		s0["output"].(map[string]any)["iceberg"].(map[string]any)["catalog"].(map[string]any)["url"])
}

func TestRunnerConfigPaths_MapsKeysToRunnerHostPaths(t *testing.T) {
	sets := []renderedPointConfigs{
		{Key: "2-a0", Single: "/local/tmp/a"},
		{Key: "2-b", Root: "/local/tmp/root", Streams: []string{"/local/tmp/s0", "/local/tmp/s1"}},
	}
	got := runnerConfigPaths(sets)
	require.Equal(t, "/opt/bench/cfg/2-a0/config.yaml", got["2-a0"].Single)
	require.Empty(t, got["2-a0"].Root)
	require.Equal(t, "/opt/bench/cfg/2-b/root.yaml", got["2-b"].Root)
	require.Equal(t, "/opt/bench/cfg/2-b/streams", got["2-b"].Dir)
	require.Empty(t, got["2-b"].Single)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run 'TestRenderPointConfigs|TestRunnerConfigPaths' -v`

Expected: FAIL — `renderPointConfigs`, `renderedPointConfigs`, `runnerConfigPaths` undefined.

- [ ] **Step 3: Write the implementation**

In `benchmarking/aws/runner/main.go`, refactor `renderPipelineConfig` into reusable pieces and add the new functions:

```go
// renderedPointConfigs is one sweep point's rendered config set, as local temp
// file paths. Single is set for single-pipeline points; Root plus Streams for
// streams-mode points.
type renderedPointConfigs struct {
	Key     string
	Single  string
	Root    string
	Streams []string
}

// writeTempYAML marshals cfg, resolves ${TF_OUTPUT} placeholders, and writes it
// to a temp file, returning the path.
func writeTempYAML(cfg map[string]any, outs map[string]string, pattern string) (string, error) {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	defer tmp.Close()
	if _, err := tmp.WriteString(substitutePlaceholders(string(raw), outs)); err != nil {
		return "", err
	}
	return tmp.Name(), nil
}

// rootSections are the observability and service-wide fields. In streams mode
// they live in the -o root config; in run mode they sit alongside the pipeline
// in the single config.
func rootSections(s *Scenario) map[string]any {
	cfg := map[string]any{
		"http": map[string]any{"debug_endpoints": true},
		"redpanda": map[string]any{
			"seed_brokers": []string{"${REDPANDA_BROKER_ENDPOINTS}"},
		},
		"logger": map[string]any{"level": "INFO"},
		"metrics": map[string]any{
			"prometheus": map[string]any{"add_process_metrics": true, "add_go_metrics": true},
		},
	}
	// Connectors that require a persistent checkpoint (e.g. mysql_cdc) declare
	// cache_resources in the scenario's pipeline block. Resources are
	// service-wide, so they belong here in both modes.
	if cr, ok := s.Pipeline["cache_resources"]; ok {
		cfg["cache_resources"] = cr
	}
	return cfg
}

// renderPointConfigs renders the launch config(s) for one sweep point. A point
// with Streams <= 1 gets a single config identical in shape to the pre-arms
// renderer. A multi-stream point gets a root config (observability only) plus
// one stream config per pipeline, each writing its own Iceberg table but
// sharing the source topic and consumer group.
func renderPointConfigs(s *Scenario, outs map[string]string, topo Topology, names BenchNames, p sweepPoint) (renderedPointConfigs, error) {
	out := renderedPointConfigs{Key: p.Key()}

	// Arms carry a merged pipeline; arm-less points use the scenario's own.
	armScenario := *s
	if p.Pipeline != nil {
		armScenario.Pipeline = p.Pipeline
	}

	if p.Streams <= 1 {
		path, err := renderPipelineConfig(&armScenario, outs, topo, names.WithStreams(1))
		if err != nil {
			return renderedPointConfigs{}, err
		}
		out.Single = path
		return out, nil
	}

	rootPath, err := writeTempYAML(rootSections(&armScenario), outs, "bench-root-*.yaml")
	if err != nil {
		return renderedPointConfigs{}, fmt.Errorf("render root config for %s: %w", out.Key, err)
	}
	out.Root = rootPath

	for i := 0; i < p.Streams; i++ {
		streamNames := names.WithStreams(p.Streams).WithStream(i)
		input, output, err := topo.Pipeline(&armScenario, streamNames)
		if err != nil {
			return renderedPointConfigs{}, fmt.Errorf("render stream %d of %s: %w", i, out.Key, err)
		}
		cfg := map[string]any{"input": input, "output": output}
		if buf, ok := armScenario.Pipeline["buffer"]; ok {
			cfg["buffer"] = buf
		}
		path, err := writeTempYAML(cfg, outs, fmt.Sprintf("bench-stream%d-*.yaml", i))
		if err != nil {
			return renderedPointConfigs{}, fmt.Errorf("write stream %d of %s: %w", i, out.Key, err)
		}
		out.Streams = append(out.Streams, path)
	}
	return out, nil
}

// runnerConfigPaths maps each point key to where its configs land on the runner
// host after staging.
func runnerConfigPaths(sets []renderedPointConfigs) map[string]pointConfigPaths {
	out := make(map[string]pointConfigPaths, len(sets))
	for _, set := range sets {
		base := "/opt/bench/cfg/" + set.Key
		var p pointConfigPaths
		if set.Single != "" {
			p.Single = base + "/config.yaml"
		} else {
			p.Root = base + "/root.yaml"
			p.Dir = base + "/streams"
		}
		out[set.Key] = p
	}
	return out
}
```

Refactor `renderPipelineConfig` to build its map from `rootSections(s)` plus `input`/`output`/`buffer` so the two paths cannot drift:

```go
func renderPipelineConfig(s *Scenario, outs map[string]string, topo Topology, names BenchNames) (string, error) {
	input, output, err := topo.Pipeline(s, names)
	if err != nil {
		return "", fmt.Errorf("render pipeline: %w", err)
	}
	cfg := rootSections(s)
	cfg["input"] = input
	cfg["output"] = output
	// A scenario may declare a top-level buffer (e.g. memory) to decouple a
	// fast input from a commit-latency-bound output like the iceberg sink.
	if buf, ok := s.Pipeline["buffer"]; ok {
		cfg["buffer"] = buf
	}
	return writeTempYAML(cfg, outs, "bench-config-*.yaml")
}
```

Now change `stageArtefacts` to take the set list. Replace its config upload and download script:

```go
func stageArtefacts(ctx context.Context, opts benchOpts, outs map[string]string, binPath string, sets []renderedPointConfigs, legacy bool) error {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]

	type upload struct{ key, path string }
	items := []upload{
		{"stage/redpanda-connect", binPath},
		{"stage/license.jwt", opts.licenseFile},
	}
	var dl []string // per-point download lines appended to the runner script

	// Arm-less runs keep the historical single stage/config.yaml →
	// /opt/bench/config.yaml path so nothing about the six existing scenarios
	// changes. Arm runs get a per-point directory.
	if legacy {
		items = append(items, upload{"stage/config.yaml", sets[0].Single})
		dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/stage/config.yaml /opt/bench/config.yaml`, bucket))
	} else {
		for _, set := range sets {
			base := "stage/cfg/" + set.Key
			host := "/opt/bench/cfg/" + set.Key
			dl = append(dl, fmt.Sprintf(`mkdir -p %s/streams`, host))
			if set.Single != "" {
				items = append(items, upload{base + "/config.yaml", set.Single})
				dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/%s/config.yaml %s/config.yaml`, bucket, base, host))
				continue
			}
			items = append(items, upload{base + "/root.yaml", set.Root})
			dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/%s/root.yaml %s/root.yaml`, bucket, base, host))
			for i, sp := range set.Streams {
				name := fmt.Sprintf("stream-%d.yaml", i)
				items = append(items, upload{fmt.Sprintf("%s/streams/%s", base, name), sp})
				dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/%s/streams/%s %s/streams/%s`, bucket, base, name, host, name))
			}
		}
	}

	for _, item := range items {
		// Both errors name the S3 key, not just the local temp path: the temp
		// paths are ephemeral /tmp/bench-stream1-<rand>.yaml names, while the key
		// carries the point key and stream index, so it is the only thing that
		// tells an operator WHICH arm and stream failed.
		f, err := os.Open(item.path)
		if err != nil {
			return fmt.Errorf("open %s for s3 key %s: %w", item.path, item.key, err)
		}
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &item.key, Body: f})
		f.Close()
		if err != nil {
			return fmt.Errorf("upload %s to %s: %w", item.path, item.key, err)
		}
	}

	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/stage/redpanda-connect /opt/bench/redpanda-connect
aws s3 cp s3://%s/stage/license.jwt /opt/bench/license.jwt
%s
chmod +x /opt/bench/redpanda-connect
chmod 0600 /opt/bench/license.jwt
aws s3 cp s3://%s/stage/iceberg-tablegen /opt/bench/iceberg-tablegen 2>/dev/null && chmod +x /opt/bench/iceberg-tablegen || true
`, bucket, bucket, strings.Join(dl, "\n"), bucket)
	return ssmExec.Run(ctx, outs["runner_instance_id"], script, streamingOnLine(os.Stdout, "stage"))
}
```

`legacy` is an explicit parameter, not inferred from the key shape: `runBench` passes `len(s.Matrix.Arms) == 0`. In the legacy branch `sets` always has exactly one element — every arm-less point shares one config, so `runBench` renders it once (see the wiring below).

Finally, wire `runBench`. Where it currently renders one config and calls `stageArtefacts(..., cfgPath)`:

```go
	plan := buildSweepPlan(s)
	legacy := len(s.Matrix.Arms) == 0
	var sets []renderedPointConfigs
	if legacy {
		// Every point shares one config; render it once, as before.
		set, err := renderPointConfigs(s, sharedOuts, topo, names, plan[0])
		if err != nil {
			return err
		}
		sets = []renderedPointConfigs{set}
	} else {
		for _, p := range plan {
			set, err := renderPointConfigs(s, sharedOuts, topo, names, p)
			if err != nil {
				return err
			}
			sets = append(sets, set)
		}
	}
	if err := stageArtefacts(ctx, opts, sharedOuts, binPath, sets, legacy); err != nil {
		return err
	}
```

Set on the `MatrixRunner`: `ConfigPath: "/opt/bench/config.yaml"` stays, and add:

```go
		ConfigPaths: func() map[string]pointConfigPaths {
			if legacy {
				return nil // every point uses ConfigPath
			}
			return runnerConfigPaths(sets)
		}(),
```

Add the arms/engines guard at the **top** of `runBench`, immediately after the
scenario is loaded — NOT just before `MatrixRunner` is constructed. It needs only
`s.Matrix.Arms` and `opts.engines`, both available immediately, and placing it
late means an invalid combination is caught only after `buildConnect`, config
rendering, `stageArtefacts` and the seeder have already run. Seeding this
scenario's 132 GB dataset is minutes of wall-clock and real S3 spend; fail before
paying for it:

```go
	if len(s.Matrix.Arms) > 0 {
		if len(opts.engines) != 1 || opts.engines[0] != "connect" {
			return fmt.Errorf("matrix.arms requires --engines=connect (got %v): arms compare Connect launch topologies, not engines", opts.engines)
		}
	}
```

Adjust the existing `renderPipelineConfig` call site and `stageArtefacts` call site so nothing else references the old signatures. Also pass the plan max stream count into the reset render:

```go
	reset, err := topo.ResetScript(s, sharedOuts, names.WithStreams(planMaxStreams(plan)))
```

- [ ] **Step 4: Run the full package**

Run: `task test:unit -- benchmarking/aws/runner`

Expected: PASS. Note `renderPointConfigs` leaves temp files behind; that already matched `renderPipelineConfig`'s behaviour, so no cleanup is added here.

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/
git commit -m "feat(bench): per-arm config rendering and staging incl. streams-mode root+stream configs"
```

---

### Task 10: Report arms in JSON and markdown

**Files:**
- Modify: `benchmarking/aws/runner/render.go:52-65` (`PointResult`), `:108-119` (`markdownRow`), `:136-199` (grouping)
- Modify: `benchmarking/aws/runner/templates/result.md.tmpl`
- Modify: `benchmarking/aws/runner/main.go:329-339` (`PointResult` construction)
- Test: `benchmarking/aws/runner/render_test.go`

**Interfaces:**
- Consumes: `SweepPoint.ArmID`/`GOMAXPROCS` (Task 8).
- Produces: `PointResult` gains `Arm string` and `GOMAXPROCS int`; `markdownRow` gains `Arm string` and `GOMAXPROCS int`.

The template's first column is currently headed `GOMAXPROCS` but renders `.VCPU`. That was harmless while the two were always equal; with arms it would print `2` under a `GOMAXPROCS` heading for the `GOMAXPROCS=4` arm. Split them into honest separate columns.

- [ ] **Step 1: Write the failing test**

Add to `benchmarking/aws/runner/render_test.go`:

```go
func TestAppendMarkdown_RendersArmRows(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "iceberg.md")
	r := &Result{
		Scenario:  "iceberg/orders-sink-streams-ab",
		GitSHA:    "abcdef1234567890",
		StartedAt: time.Now().UTC(),
		Points: []PointResult{
			{VCPU: 2, GOMAXPROCS: 2, Arm: "a0-1pipe-gmp2", Engine: "connect",
				Summary: Summary{MedianMBPerSec: 69, MeanMBPerSec: 69.1}},
			{VCPU: 2, GOMAXPROCS: 4, Arm: "a1-1pipe-gmp4", Engine: "connect",
				Summary: Summary{MedianMBPerSec: 80, MeanMBPerSec: 80.2}},
			{VCPU: 2, GOMAXPROCS: 4, Arm: "b-2pipe-gmp4", Engine: "connect",
				Summary: Summary{MedianMBPerSec: 95, MeanMBPerSec: 95.3}},
		},
	}
	require.NoError(t, AppendMarkdown(target, r, "arms A/B"))
	raw, err := os.ReadFile(target)
	require.NoError(t, err)
	out := string(raw)

	// One row per arm, all three present and distinguishable.
	for _, arm := range []string{"a0-1pipe-gmp2", "a1-1pipe-gmp4", "b-2pipe-gmp4"} {
		require.Contains(t, out, arm)
	}
	require.Contains(t, out, "| vCPU |", "vCPU and GOMAXPROCS must be separate columns")
	require.Contains(t, out, "arm")
	// Count "| connect" (the table's engine column), not bare "connect": the
	// template's Git-SHA link contains github.com/redpanda-data/connect/commit/…,
	// so a bare count is 4 and would pass even if a table row went missing.
	require.Equal(t, 3, strings.Count(out, "| connect"), "three connect rows, one per arm")
}

func TestAppendMarkdown_ArmlessRowsKeepBlankArm(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "postgres.md")
	r := &Result{
		Scenario:  "postgres/orders-cdc",
		GitSHA:    "abcdef1234567890",
		StartedAt: time.Now().UTC(),
		Points: []PointResult{
			{VCPU: 1, GOMAXPROCS: 1, Engine: "connect", Summary: Summary{MedianMBPerSec: 10}},
			{VCPU: 1, GOMAXPROCS: 1, Engine: "kafka_connect", Summary: Summary{MedianMBPerSec: 8}},
		},
	}
	require.NoError(t, AppendMarkdown(target, r, "cdc"))
	raw, err := os.ReadFile(target)
	require.NoError(t, err)
	out := string(raw)
	// The KC delta column still works: connect and KC group together when
	// neither carries an arm.
	require.Contains(t, out, "-2 MB/s (-20%)")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run TestAppendMarkdown -v`

Expected: FAIL to compile — `PointResult.Arm` / `.GOMAXPROCS` undefined.

- [ ] **Step 3: Write the implementation**

In `benchmarking/aws/runner/render.go`, add to `PointResult`:

```go
	// Arm is the matrix.arms id this point measured; empty for arm-less sweeps.
	Arm string `json:"arm,omitempty"`
	// GOMAXPROCS is the runtime P count measured at this point. Equal to VCPU
	// unless an arm oversubscribed it.
	GOMAXPROCS int `json:"gomaxprocs,omitempty"`
```

Add to `markdownRow`:

```go
	GOMAXPROCS int
	Arm        string
```

Replace the grouping in `AppendMarkdown` so the key includes the arm:

```go
	// First pass: group points by (vCPU, arm) and capture iteration order. The
	// arm is part of the key so an A/B at one vCPU yields one row group per
	// arm; for arm-less runs the arm is "" and grouping is by vCPU as before,
	// which keeps the connect/KC pairing (and the delta column) intact.
	type groupKey struct {
		vcpu int
		arm  string
	}
	type vGroup struct {
		key      groupKey
		byEngine map[string]PointResult
	}
	groups := map[groupKey]*vGroup{}
	var order []groupKey
	for _, p := range r.Points {
		k := groupKey{vcpu: p.VCPU, arm: p.Arm}
		g, ok := groups[k]
		if !ok {
			g = &vGroup{key: k, byEngine: map[string]PointResult{}}
			groups[k] = g
			order = append(order, k)
		}
		g.byEngine[p.Engine] = p
	}
```

Change the second pass's loop head to `for _, k := range order { g := groups[k]; ... }`, and add to the `markdownRow` literal:

```go
				GOMAXPROCS:     p.GOMAXPROCS,
				Arm:            p.Arm,
```

In `benchmarking/aws/runner/main.go`, add to the `PointResult` construction in `runBench`:

```go
			Arm:          p.ArmID,
			GOMAXPROCS:   p.GOMAXPROCS,
```

In `benchmarking/aws/runner/templates/result.md.tmpl`, replace the throughput table header and row:

```
| vCPU | GOMAXPROCS | arm            | engine        | MB/sec (p50) | mean MB/s    | mean msg/s    | broker MB/s | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) | Δ vs Connect       |
|------|------------|----------------|---------------|--------------|--------------|---------------|-------------|-------------|--------------|---------------|--------------------|
{{- range .Rows }}
| {{printf "%-4d" .VCPU}} | {{printf "%-10d" .GOMAXPROCS}} | {{printf "%-14s" .Arm}} | {{printf "%-13s" .Engine}} | {{.MedianMB}} | {{.MeanMB}} | {{printf "%13s" .MeanMsg}} | {{.BrokerMedianMB}} | {{.P5MB}} | {{.P95MB}} | {{printf "%13s" .MedianMsgFC}} | {{printf "%-18s" .DeltaVsConnect}} |
{{- end }}
```

`benchmarking/aws/runner/summary.go` needs no change: `derivedRow` picks the highest-median Connect point, which for an arms run is the winning arm — the desired SUMMARY behaviour.

- [ ] **Step 4: Run the full package**

Run: `task test:unit -- benchmarking/aws/runner`

Expected: PASS, including pre-existing `render_test.go` and `summary_test.go` cases. If an existing render test asserts the old header text, update it to the new columns — the numbers it checks must not change.

- [ ] **Step 5: Commit**

```bash
gofmt -l -w $(git diff --name-only -- 'benchmarking/aws/runner/*.go')
go vet ./benchmarking/aws/runner/
git add benchmarking/aws/runner/
git commit -m "feat(bench): report arm and GOMAXPROCS in result JSON and markdown"
```

---

### Task 11: The A/B scenario, validation, and a local streams-mode smoke

**Files:**
- Create: `benchmarking/aws/scenarios/iceberg/orders-sink-streams-ab.yaml`
- Test: `task aws:validate` plus a local `redpanda-connect streams` start check

**Interfaces:**
- Consumes: everything above.
- Produces: the scenario the AWS run will use.

This task is where the one genuinely unverified assumption gets tested: that Connect's streams-mode root config accepts the `redpanda` section and that a stream config carrying only `input`/`buffer`/`output` is valid. Do it locally, before any AWS spend.

- [ ] **Step 1: Write the scenario**

Create `benchmarking/aws/scenarios/iceberg/orders-sink-streams-ab.yaml`:

```yaml
name: iceberg-orders-sink-streams-ab
description: |
  A/B at a fixed 2-vCPU pin: does splitting one iceberg pipeline into two
  streams-mode pipelines buy throughput when GOMAXPROCS oversubscribes the core
  allocation? Connect counts licensed cores off the machine CPU rather than
  GOMAXPROCS, so raising it is free; the iceberg output is commit-latency-bound
  (Glue REST + S3), so blocked goroutines can otherwise leave the pinned cores
  idle.

  Three arms, all pinned to the same two cores, all Connect-only, all with the
  same vCPU-derived GOMEMLIMIT:
    a0-1pipe-gmp2  in-session baseline (GOMAXPROCS == cores, as every prior sweep)
    a1-1pipe-gmp4  isolates the GOMAXPROCS oversubscription effect
    b-2pipe-gmp4   adds the pipeline split on top

  Arm B halves each stream's buffer and max_in_flight so total buffered memory
  (500 MiB) and total in-flight budget (16) match arms A — a pure topology
  comparison, not a resource-budget one. Both streams consume the same topic
  under the same consumer group (16 partitions split 8/8) and each writes its
  own Iceberg table; the arm's throughput is the summed committed-bytes growth.

  Base config is Recipe A from docs/benchmark-results/iceberg-recipe-comparison.md,
  which won at 2 vCPU (69.1 vs 64.1 MB/s).

direction: sink
connector: iceberg
stack: iceberg

infra:
  runner:
    instance_type: c8g.4xlarge

dataset:
  # 110M x 1200 B = ~132 GB = 125,885 MiB. The window is 15 min at 0 s warmup,
  # so the topic must hold 900 s x peak. A drained topic silently deflates the
  # arm's mean instead of failing, so size for the upside: 132 GB sustains
  # ~140 MiB/s for the full window, and Scenario.Validate's bounded-dataset
  # check (total MiB / expected_peak_mb_s >= 15 min) estimates 946 s. 80 GB or
  # 96 GB would drain around 107 MiB/s — exactly the outcome under test.
  initial_rows: 110000000
  row_size_bytes: 1200
  seeder: json-orders
  expected_peak_mb_s: 133

pipeline:
  # Recipe A (order-preserving) + output batching, per-stream. Arm B's override
  # halves the buffer limit and max_in_flight so the totals match arms A.
  buffer:
    memory:
      limit: 524288000   # 500 MiB
      batch_policy:
        count: 10000
        period: 10s
  output:
    iceberg:
      max_in_flight: 16
      batching:
        count: 10000
        period: 10s
      commit:
        max_snapshot_age: 24h

matrix:
  cpu_points: [2]
  arms:
    - id: a0-1pipe-gmp2
      gomaxprocs: 2
      streams: 1
    - id: a1-1pipe-gmp4
      gomaxprocs: 4
      streams: 1
    - id: b-2pipe-gmp4
      gomaxprocs: 4
      streams: 2
      pipeline:
        buffer:
          memory:
            limit: 262144000   # 250 MiB x 2 streams == 500 MiB total
        output:
          iceberg:
            max_in_flight: 8   # 8 x 2 streams == 16 total
```

- [ ] **Step 2: Validate the scenario**

Run: `task aws:validate scenario=iceberg/orders-sink-streams-ab`

Expected: success, no AWS calls. If it reports a validation error, fix the scenario or the Task 1 validation — do not proceed.

- [ ] **Step 3: Verify streams-mode config shapes against the real binary**

This is the free check that the streams-mode split is actually accepted. Build the binary and hand it a root config plus two stream configs shaped exactly like `renderPointConfigs` emits, using a `generate` input and a `drop` output so nothing external is needed.

```bash
go build -o /tmp/rpcn ./cmd/redpanda-connect

mkdir -p /tmp/ab-smoke/streams
cat > /tmp/ab-smoke/root.yaml <<'YAML'
http:
  debug_endpoints: true
redpanda:
  seed_brokers: ["localhost:9092"]
logger:
  level: INFO
metrics:
  prometheus:
    add_process_metrics: true
    add_go_metrics: true
YAML

for i in 0 1; do
cat > /tmp/ab-smoke/streams/stream-$i.yaml <<YAML
input:
  generate:
    interval: 1s
    mapping: 'root.i = $i'
buffer:
  memory:
    limit: 262144000
    batch_policy:
      count: 10000
      period: 10s
output:
  drop: {}
YAML
done

GOMAXPROCS=4 /tmp/rpcn streams -o /tmp/ab-smoke/root.yaml /tmp/ab-smoke/streams &
RPCN=$!
sleep 8
curl -s localhost:4195/ready; echo
curl -s localhost:4195/streams; echo
kill -TERM $RPCN
```

Expected: the process stays up, `/streams` lists two stream ids (`stream-0`, `stream-1`), and no config error is logged.

**If the root config rejects the `redpanda` section:** drop `redpanda` from `rootSections` for the streams path only (it configures Connect's own logs/status topic, not the pipeline — the `redpanda` *input* carries its own `seed_brokers`), re-run this check, and record the asymmetry in a comment in `rootSections`. Do not paper over any other config error — investigate it.

- [ ] **Step 4: Confirm the rendered configs match what the smoke validated**

Run: `go test ./benchmarking/aws/runner/ -run TestRenderPointConfigs -v`

Expected: PASS. Cross-read the assertions against the YAML you just fed the binary: the root config carries `http`/`logger`/`metrics` (+`redpanda` unless Step 3 forced its removal) and no `input`/`output`; each stream config carries `input`/`buffer`/`output` and no root fields.

- [ ] **Step 5: Full test suite and lint**

Run:
```bash
task test:unit -- benchmarking/aws/runner
go vet ./benchmarking/aws/runner/          # must be clean
gofmt -l benchmarking/aws/runner/          # see note below
```

`go vet` and the suite must be clean. `gofmt -l` will list
`benchmarking/aws/runner/doc.go` and `ssm.go` — that drift is **pre-existing and
unrelated to this plan**. Leave those two files alone; the requirement is only
that no file THIS plan touched appears in the list.

Then run the linter over this package **advisorily** — `task lint` excludes
`benchmarking/`, so this path has never been linted and pre-existing hits are
expected:

```bash
./bin/golangci-lint run ./benchmarking/aws/runner/... || true
```

Report what it says in your report. Fix only findings in code THIS plan added.
Do NOT fix pre-existing findings in untouched files, and do not treat them as
blocking — a gate that fails on pre-existing conditions is not wanted here.
One known case: `sweepplan.go`'s `max := 1` shadows the Go 1.21+ builtin and
`predeclared` is enabled, but `stats.go:179` already does the same in this
package, so it is existing convention.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/scenarios/iceberg/orders-sink-streams-ab.yaml
git commit -m "feat(bench): iceberg 1-pipeline vs 2-streams A/B scenario at 2 vCPU"
```

---

## Running the bench (after all tasks land)

Not part of the implementation — this is the handoff. Costs ~1.5 h and ~$6-8.

```bash
cd benchmarking/aws && \
  unset AWS_PROFILE && \
  aws-vault exec AWSAdministratorAccess-605419575229 -- \
    env REDPANDA_LICENSE_FILEPATH=/Users/prakhar.garg/Documents/connect_prakhar/rpcn.license \
    task aws:bench scenario=iceberg/orders-sink-streams-ab engines=connect
```

Note `task aws:bench` only resolves from `benchmarking/aws/` — the root Taskfile
does not include that namespace. The aws-vault profile is
`AWSAdministratorAccess-605419575229`; there is no profile literally named `bench`.

`--engines=connect` is mandatory (Task 9 enforces it). Interrupt only with SIGINT.

Acceptance checks on the result, from `benchmarking/aws/results/iceberg/orders-sink-streams-ab/<ts>.json`:

- No arm's `broker_series` plateaus before the window ends. A flat tail means the topic drained and that arm's mean is deflated — raise `initial_rows` and rerun.
- Arm B's two tables both grew: check `s3://<bucket>/runs/<sess>/iceberg-2-b-2pipe-gmp4-connect.txt` frames climb steadily, and confirm in Glue that both `..._connect_s0` and `..._connect_s1` hold data. A zero on one means the rebalance gave one stream no partitions.
- `s3://<bucket>/runs/<sess>/sweep-2-b-2pipe-gmp4.log` shows two distinct stream ids.
- All three arms report `GOMAXPROCS` correctly in the markdown table (2, 4, 4) against a constant `vCPU` of 2.
