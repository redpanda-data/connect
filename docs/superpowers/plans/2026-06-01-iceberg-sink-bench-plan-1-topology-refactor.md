# Iceberg Sink Bench — Plan 1: Topology Refactor

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the bench runner's source-vs-sink assumption behind a `Topology` interface so the existing CDC path runs through a `sourceTopology` with zero behavior change — preparing the seam Plan 2 fills with the Iceberg `sinkTopology`.

**Architecture:** Add a `Direction` field to `Scenario` (defaulting to `source`, so every existing scenario is unchanged). Introduce a `Topology` interface with one implementation, `sourceTopology`, whose methods *delegate to the existing free functions verbatim* (`renderSeedScript`, `renderWorkloadScript`, `combineReset`, the `renderPipelineConfig` input/output construction, and `ParseTopicSeries`+`AttributeByEngine`). Thread the selected topology through `runBench` and `MatrixRunner`. The existing runner unit tests are the regression net — they must pass unchanged.

**Tech Stack:** Go (standard library + `gopkg.in/yaml.v3`), the existing `benchmarking/aws/runner` package. Tests: `go test` table/contains style already used in the package.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `benchmarking/aws/runner/topology.go` (**create**) | `Direction` type + consts, `BenchNames` value, `MetricInputs`, the `Topology` interface, and `topologyFor` selector. |
| `benchmarking/aws/runner/topology_source.go` (**create**) | `sourceTopology` — implements `Topology` by delegating to the existing CDC free functions. |
| `benchmarking/aws/runner/topology_test.go` (**create**) | Unit tests for `BenchNames`, `topologyFor`, and `sourceTopology` method equivalence. |
| `benchmarking/aws/runner/scenario.go` (**modify**) | Add `Direction` field; default + validate it. |
| `benchmarking/aws/runner/scenario_test.go` (**modify**) | Tests for direction default + invalid-direction error. |
| `benchmarking/aws/runner/main.go` (**modify**) | Select topology; route pipeline/seed/workload/reset through it; pass topology+names into `MatrixRunner`. |
| `benchmarking/aws/runner/matrix.go` (**modify**) | `MatrixRunner` gains `Topology` + `Names`; `fetchBrokerSeriesForEngine` calls `Topology.EngineSeries`. |

The existing `scripts.go` and `brokermetrics.go` free functions are **not deleted** — `sourceTopology` calls them. This keeps the refactor minimal and the existing tests (`scripts_test.go`, `brokermetrics_test.go`) valid as-is.

---

## Task 1: Add `Direction` to the scenario schema

**Files:**
- Modify: `benchmarking/aws/runner/scenario.go` (struct at line 44-60; `LoadScenario` at 137-153; `Validate` at 155-211)
- Test: `benchmarking/aws/runner/scenario_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `benchmarking/aws/runner/scenario_test.go`:

```go
func TestLoadScenario_DirectionDefaultsToSource(t *testing.T) {
	s := &Scenario{Direction: ""}
	s.applyDirectionDefault()
	if s.Direction != DirectionSource {
		t.Errorf("empty direction must default to source, got %q", s.Direction)
	}
}

func TestValidate_RejectsUnknownDirection(t *testing.T) {
	s := &Scenario{
		Name:      "x",
		Connector: "postgres_cdc",
		Stack:     "postgres",
		Direction: "sideways",
		Infra:     InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Matrix:    MatrixSpec{CPUPoints: []int{1}},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute},
	}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "direction") {
		t.Fatalf("expected a direction error, got %v", err)
	}
}
```

If `scenario_test.go` does not already import `strings`/`time`, add them.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./benchmarking/aws/runner/ -run 'TestLoadScenario_DirectionDefaultsToSource|TestValidate_RejectsUnknownDirection' -v`
Expected: compile error / FAIL — `Direction`, `DirectionSource`, and `applyDirectionDefault` are undefined.

- [ ] **Step 3: Add the `Direction` type and field**

In `benchmarking/aws/runner/scenario.go`, add above the `Scenario` struct (after the `const` block at line 21):

```go
// Direction is the role the connector-under-test plays in the pipeline.
// Source connectors read an external system and write into Redpanda (CDC);
// sink connectors read from Redpanda and write into an external system.
type Direction string

const (
	DirectionSource Direction = "source"
	DirectionSink   Direction = "sink"
)
```

Add the field to the `Scenario` struct (after the `Connector` field, line 47):

```go
	Connector   string         `yaml:"connector"`
	Direction   Direction      `yaml:"direction,omitempty"`
```

- [ ] **Step 4: Add the default helper and call it from `LoadScenario`**

Add this method near `Validate` in `scenario.go`:

```go
// applyDirectionDefault sets the implicit "source" direction so existing CDC
// scenarios (which omit the field) keep their behavior.
func (s *Scenario) applyDirectionDefault() {
	if s.Direction == "" {
		s.Direction = DirectionSource
	}
}
```

In `LoadScenario`, call it *before* `Validate` (between the `yaml.Unmarshal` at line 143-145 and the `Validate` call at 146):

```go
	s.applyDirectionDefault()
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
```

- [ ] **Step 5: Validate the direction value**

In `Validate`, immediately after the `Connector` check (after line 161, before the `engineSpecFor` check at 162):

```go
	switch s.Direction {
	case DirectionSource, DirectionSink, "":
		// "" is tolerated for direct struct construction in tests; LoadScenario
		// normalizes it via applyDirectionDefault.
	default:
		return fmt.Errorf("direction %q is invalid; must be %q or %q", s.Direction, DirectionSource, DirectionSink)
	}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./benchmarking/aws/runner/ -run 'TestLoadScenario_DirectionDefaultsToSource|TestValidate_RejectsUnknownDirection' -v`
Expected: PASS.

- [ ] **Step 7: Run the full package suite (regression net)**

Run: `go test ./benchmarking/aws/runner/`
Expected: PASS (no existing test references `Direction`, so all stay green).

- [ ] **Step 8: Commit**

```bash
git add benchmarking/aws/runner/scenario.go benchmarking/aws/runner/scenario_test.go
git commit -m "feat(bench): add scenario direction field (defaults to source)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `BenchNames` value

**Files:**
- Create: `benchmarking/aws/runner/topology.go`
- Test: `benchmarking/aws/runner/topology_test.go`

`BenchNames` centralizes the per-engine resource names currently built by scattered `fmt.Sprintf` calls. In Plan 1 it carries `SessionID`+`Connector` and exposes the two names the source path needs; Plan 2 extends it with Iceberg table names.

- [ ] **Step 1: Write the failing test**

Create `benchmarking/aws/runner/topology_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestBenchNames_SourceTopicConventions(t *testing.T) {
	n := newBenchNames("sess-abc", "postgres_cdc")
	if got := n.ConnectTopic(); got != "bench_sess-abc_postgres_cdc_connect" {
		t.Errorf("ConnectTopic = %q, want bench_sess-abc_postgres_cdc_connect", got)
	}
	if got := n.KCTopicPrefix(); got != "bench_sess-abc_postgres_cdc_kc" {
		t.Errorf("KCTopicPrefix = %q, want bench_sess-abc_postgres_cdc_kc", got)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./benchmarking/aws/runner/ -run TestBenchNames_SourceTopicConventions -v`
Expected: FAIL — `newBenchNames` undefined.

- [ ] **Step 3: Implement `BenchNames`**

Create `benchmarking/aws/runner/topology.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"io"
)

// BenchNames is the single source of truth for the per-session, per-engine
// resource names a bench uses. The same naming conventions were previously
// duplicated across renderPipelineConfig, combineReset, buildKCRenderInputs,
// and AttributeByEngine.
type BenchNames struct {
	SessionID string
	Connector string
}

func newBenchNames(sessionID, connector string) BenchNames {
	return BenchNames{SessionID: sessionID, Connector: connector}
}

// ConnectTopic is the single topic Connect writes to in a source bench.
func (n BenchNames) ConnectTopic() string {
	return fmt.Sprintf("bench_%s_%s_connect", n.SessionID, n.Connector)
}

// KCTopicPrefix is the Debezium topic.prefix for a source bench; KC emits
// <prefix>.<schema>.<table> topics under it.
func (n BenchNames) KCTopicPrefix() string {
	return fmt.Sprintf("bench_%s_%s_kc", n.SessionID, n.Connector)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./benchmarking/aws/runner/ -run TestBenchNames_SourceTopicConventions -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_test.go
git commit -m "feat(bench): add BenchNames naming helper

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: The `Topology` interface, `MetricInputs`, and selector

**Files:**
- Modify: `benchmarking/aws/runner/topology.go`
- Test: `benchmarking/aws/runner/topology_test.go`

- [ ] **Step 1: Write the failing test**

Add to `benchmarking/aws/runner/topology_test.go`:

```go
func TestTopologyFor(t *testing.T) {
	if _, err := topologyFor(DirectionSource); err != nil {
		t.Errorf("source topology must resolve, got %v", err)
	}
	if _, err := topologyFor(Direction("")); err != nil {
		t.Errorf("empty direction must resolve to source, got %v", err)
	}
	if _, err := topologyFor(DirectionSink); err == nil {
		t.Errorf("sink topology is not implemented in Plan 1; expected an error")
	}
	if _, err := topologyFor(Direction("sideways")); err == nil {
		t.Errorf("unknown direction must error")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./benchmarking/aws/runner/ -run TestTopologyFor -v`
Expected: FAIL — `topologyFor` and `Topology` undefined.

- [ ] **Step 3: Define the interface, `MetricInputs`, and selector**

Append to `benchmarking/aws/runner/topology.go`:

```go
// MetricInputs carries everything EngineSeries needs to turn a per-engine
// metrics dump into a throughput series. Body is the raw dump for one
// (engine, vCPU) point: the Redpanda /public_metrics scrape for a source
// bench; the Iceberg snapshot poll for a sink bench (Plan 2).
type MetricInputs struct {
	Body  io.Reader
	Names BenchNames
}

// Topology abstracts the direction-specific wiring of a bench. One
// implementation exists per Direction. All source-vs-sink branching lives
// behind this interface; callers (runBench, MatrixRunner) are direction-blind.
type Topology interface {
	// Pipeline returns the Connect input and output config maps.
	Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error)
	// SeedScript renders the load-gen script that primes the system with data.
	SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error)
	// WorkloadScript renders the sustained-load script, or "" when none.
	WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error)
	// ResetScript renders the between-points reset script.
	ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error)
	// EngineSeries turns a per-engine metrics dump into a throughput series.
	EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error)
}

// topologyFor selects the implementation for a scenario's direction. An empty
// direction is treated as source (LoadScenario normalizes it, but direct
// struct construction in tests may leave it blank).
func topologyFor(d Direction) (Topology, error) {
	switch d {
	case DirectionSource, "":
		return sourceTopology{}, nil
	case DirectionSink:
		return nil, fmt.Errorf("sink topology not yet implemented (lands in Plan 2)")
	default:
		return nil, fmt.Errorf("unknown direction %q", d)
	}
}
```

- [ ] **Step 4: Run to verify it fails differently**

Run: `go test ./benchmarking/aws/runner/ -run TestTopologyFor -v`
Expected: compile error — `sourceTopology` undefined. (Task 4 defines it.)

- [ ] **Step 5: Commit (after Task 4 makes it compile)**

Do not commit yet — the package will not compile until `sourceTopology` exists. Proceed to Task 4, then commit there.

---

## Task 4: `sourceTopology` delegating to existing functions

**Files:**
- Create: `benchmarking/aws/runner/topology_source.go`
- Test: `benchmarking/aws/runner/topology_test.go`

`sourceTopology` must produce byte-identical output to the current code paths. Each method delegates to an existing free function (or, for `Pipeline`, reproduces the exact map `renderPipelineConfig` builds today).

- [ ] **Step 1: Write the failing equivalence tests**

Add to `benchmarking/aws/runner/topology_test.go`:

```go
import (
	"strings"
	"testing"
	"time"
)

func TestSourceTopology_SeedScript_MatchesRenderSeedScript(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows", InitialRows: 1000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db", "results_bucket": "bucket"}
	n := newBenchNames("sess", "postgres_cdc")

	want, err := renderSeedScript(s, outs, "stage/cdc-rows")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	got, err := sourceTopology{}.SeedScript(s, outs, n)
	if err != nil {
		t.Fatalf("SeedScript: %v", err)
	}
	if got != want {
		t.Errorf("SeedScript diverged from renderSeedScript:\n got: %q\nwant: %q", got, want)
	}
}

func TestSourceTopology_ResetScript_MatchesCombineReset(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Reset: []ResetStep{{SQL: "SELECT 1"}}}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host/db"}
	n := newBenchNames("sess", "postgres_cdc")

	want, err := combineReset(s.Connector, s.Reset, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	got, err := sourceTopology{}.ResetScript(s, outs, n)
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	if got != want {
		t.Errorf("ResetScript diverged from combineReset:\n got: %q\nwant: %q", got, want)
	}
}

func TestSourceTopology_WorkloadScript_NilIsEmpty(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Workload: nil}
	got, err := sourceTopology{}.WorkloadScript(s, map[string]string{}, newBenchNames("sess", "postgres_cdc"))
	if err != nil {
		t.Fatalf("WorkloadScript: %v", err)
	}
	if got != "" {
		t.Errorf("nil workload must render empty, got %q", got)
	}
}

func TestSourceTopology_Pipeline_InputAndOutput(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input": map[string]any{"postgres_cdc": map[string]any{"dsn": "x"}},
		},
	}
	in, out, err := sourceTopology{}.Pipeline(s, newBenchNames("sess", "postgres_cdc"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	if _, ok := in["postgres_cdc"]; !ok {
		t.Errorf("input must pass through scenario pipeline.input; got %#v", in)
	}
	rp, ok := out["redpanda"].(map[string]any)
	if !ok {
		t.Fatalf("output must contain a redpanda block; got %#v", out)
	}
	if rp["topic"] != "bench_${BENCH_SESSION_ID}_postgres_cdc_connect" {
		t.Errorf("output topic = %v, want bench_${BENCH_SESSION_ID}_postgres_cdc_connect", rp["topic"])
	}
}

func TestSourceTopology_EngineSeries_ParsesBrokerDump(t *testing.T) {
	// One topic owned by Connect, sampled across two frames so a single
	// throughput point is produced. Frame format matches parseSnapshots.
	dump := strings.Join([]string{
		"# t=1000",
		`redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_namespace="kafka",redpanda_topic="bench_sess_postgres_cdc_connect"} 0`,
		"# t=1001",
		`redpanda_kafka_request_bytes_total{redpanda_request="produce",redpanda_namespace="kafka",redpanda_topic="bench_sess_postgres_cdc_connect"} 1048576`,
	}, "\n")
	in := MetricInputs{Body: strings.NewReader(dump), Names: newBenchNames("sess", "postgres_cdc")}
	pts, err := sourceTopology{}.EngineSeries(in, "connect")
	if err != nil {
		t.Fatalf("EngineSeries: %v", err)
	}
	if len(pts) != 1 {
		t.Fatalf("expected 1 throughput point, got %d (%#v)", len(pts), pts)
	}
	if pts[0].MBPerSec <= 0 {
		t.Errorf("expected positive MB/s, got %v", pts[0].MBPerSec)
	}
}
```

> Note: the `# t=NNNN` frame-marker format in `TestSourceTopology_EngineSeries_ParsesBrokerDump` must match what `parseSnapshots` expects. Before running, open `benchmarking/aws/runner/prom.go`, find the timestamp-marker prefix `parseSnapshots` parses, and adjust the two `# t=...` lines to that exact format if it differs.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./benchmarking/aws/runner/ -run TestSourceTopology -v`
Expected: compile error — `sourceTopology` undefined.

- [ ] **Step 3: Implement `sourceTopology`**

Create `benchmarking/aws/runner/topology_source.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "fmt"

// sourceTopology is the CDC bench path: the connector-under-test reads an
// external system and writes into Redpanda; throughput is measured on the
// broker produce side. Every method delegates to the pre-existing free
// functions so behavior is byte-identical to the pre-refactor runner.
type sourceTopology struct{}

func (sourceTopology) Pipeline(s *Scenario, n BenchNames) (input, output map[string]any, err error) {
	in, ok := s.Pipeline["input"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("source scenario %q: pipeline.input must be a map", s.Connector)
	}
	// Per-engine output topic so broker-side metrics attribute cleanly. The
	// ${BENCH_SESSION_ID} placeholder is substituted later by
	// substitutePlaceholders — preserved verbatim from renderPipelineConfig.
	out := map[string]any{
		"processors": []any{
			map[string]any{
				"benchmark": map[string]any{"interval": "1s", "count_bytes": true},
			},
		},
		"redpanda": map[string]any{
			"topic": fmt.Sprintf("bench_${BENCH_SESSION_ID}_%s_connect", s.Connector),
		},
	}
	return in, out, nil
}

func (sourceTopology) SeedScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return renderSeedScript(s, outs, "stage/"+s.Dataset.Seeder)
}

func (sourceTopology) WorkloadScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return renderWorkloadScript(s, outs)
}

func (sourceTopology) ResetScript(s *Scenario, outs map[string]string, n BenchNames) (string, error) {
	return combineReset(s.Connector, s.Reset, outs)
}

func (sourceTopology) EngineSeries(in MetricInputs, engine string) ([]TopicPoint, error) {
	series, err := ParseTopicSeries(in.Body)
	if err != nil {
		return nil, err
	}
	byEngine, err := AttributeByEngine(series, in.Names.SessionID, in.Names.Connector)
	if err != nil {
		return nil, err
	}
	return byEngine[engine], nil
}
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `go test ./benchmarking/aws/runner/ -run 'TestSourceTopology|TestTopologyFor' -v`
Expected: PASS. If `TestSourceTopology_EngineSeries_ParsesBrokerDump` fails on parsing, fix the `# t=` marker in the test fixture to match `prom.go` (see Step 1 note) — not the implementation.

- [ ] **Step 5: Run the full package suite**

Run: `go test ./benchmarking/aws/runner/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/topology.go benchmarking/aws/runner/topology_source.go benchmarking/aws/runner/topology_test.go
git commit -m "feat(bench): add Topology interface + sourceTopology

Wraps the existing CDC free functions behind a direction-blind
interface. No behavior change; equivalence pinned by tests.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Route `runBench` through the topology

**Files:**
- Modify: `benchmarking/aws/runner/main.go` (`runBench` 114-358; `renderPipelineConfig` 532-578; `runSeeder` 713-754)

This task changes wiring only. The existing integration behavior is unchanged; correctness is confirmed by the package suite (Step 6) plus `task aws:validate` (Step 7).

- [ ] **Step 1: Change `renderPipelineConfig` to take the topology + names**

Replace the signature and the `input`/`output` lines of `renderPipelineConfig` (lines 532-557). New version:

```go
func renderPipelineConfig(s *Scenario, outs map[string]string, topo Topology, names BenchNames) (string, error) {
	input, output, err := topo.Pipeline(s, names)
	if err != nil {
		return "", fmt.Errorf("render pipeline: %w", err)
	}
	cfg := map[string]any{
		"http": map[string]any{"debug_endpoints": true},
		"redpanda": map[string]any{
			"seed_brokers": []string{"${REDPANDA_BROKER_ENDPOINTS}"},
		},
		"input":  input,
		"output": output,
		"logger": map[string]any{"level": "INFO"},
		"metrics": map[string]any{
			"prometheus": map[string]any{"add_process_metrics": true, "add_go_metrics": true},
		},
	}
	// Connectors that require a persistent checkpoint (e.g. mysql_cdc) declare
	// cache_resources in the scenario's pipeline block. Thread them through
	// to the Connect config root when present.
	if cr, ok := s.Pipeline["cache_resources"]; ok {
		cfg["cache_resources"] = cr
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return "", err
	}
	rendered := substitutePlaceholders(string(raw), outs)
	tmp, err := os.CreateTemp("", "bench-config-*.yaml")
	if err != nil {
		return "", err
	}
	defer tmp.Close()
	if _, err := tmp.WriteString(rendered); err != nil {
		return "", err
	}
	return tmp.Name(), nil
}
```

- [ ] **Step 2: Select the topology and compute names in `runBench`**

In `runBench`, right after the scenario loads and the license check passes (after line 129 `fmt.Printf("[1/7] loaded scenario %s\n", s.Name)`):

```go
	topo, err := topologyFor(s.Direction)
	if err != nil {
		return err
	}
```

After `sessionID := newSessionID()` (line 157), add:

```go
	names := newBenchNames(sessionID, s.Connector)
```

- [ ] **Step 3: Pass topology + names to `renderPipelineConfig`**

Change line 212 from:

```go
	cfgPath, err := renderPipelineConfig(s, sharedOuts)
```

to:

```go
	cfgPath, err := renderPipelineConfig(s, sharedOuts, topo, names)
```

- [ ] **Step 4: Route seed through the topology**

Change `runSeeder`'s signature (line 713) and the `renderSeedScript` call inside it (line 749).

Signature:

```go
func runSeeder(ctx context.Context, opts benchOpts, s *Scenario, outs map[string]string, topo Topology, names BenchNames) error {
```

Inside, replace lines 749-752:

```go
	script, err := renderSeedScript(s, outs, key)
	if err != nil {
		return err
	}
```

with:

```go
	script, err := topo.SeedScript(s, outs, names)
	if err != nil {
		return err
	}
```

Update the call site at line 221:

```go
	if err := runSeeder(ctx, opts, s, sharedOuts, topo, names); err != nil {
```

> Note: `runSeeder` still uploads the seeder binary under the key `"stage/" + s.Dataset.Seeder`, which is exactly the key `sourceTopology.SeedScript` reconstructs internally. The `key` local in `runSeeder` (line 739) is still used for the upload; only the script-render call changes.

- [ ] **Step 5: Route reset + workload through the topology**

Replace lines 280-287:

```go
	reset, err := combineReset(s.Connector, s.Reset, sharedOuts)
	if err != nil {
		return err
	}
	workload, err := renderWorkloadScript(s, sharedOuts)
	if err != nil {
		return err
	}
```

with:

```go
	reset, err := topo.ResetScript(s, sharedOuts, names)
	if err != nil {
		return err
	}
	workload, err := topo.WorkloadScript(s, sharedOuts, names)
	if err != nil {
		return err
	}
```

- [ ] **Step 6: Set `Topology` + `Names` on the MatrixRunner**

In the `mr := &MatrixRunner{ ... }` literal (lines 264-279), add two fields (after `ScenarioConnector: s.Connector,`):

```go
		ScenarioConnector:        s.Connector,
		Topology:                 topo,
		Names:                    names,
```

(These fields are added to the struct in Task 6. The package will not compile until Task 6 is done; do both before running tests.)

- [ ] **Step 7: Proceed to Task 6 before building** — `MatrixRunner.Topology`/`Names` don't exist yet.

---

## Task 6: `MatrixRunner` consumes `Topology.EngineSeries`

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go` (struct at ~34-58; `fetchBrokerSeriesForEngine` at 277-310)

- [ ] **Step 1: Add the fields to `MatrixRunner`**

In the `MatrixRunner` struct, after the `ScenarioConnector string` field (line 57):

```go
	ScenarioConnector string
	// Topology supplies the direction-specific metric parser used by
	// fetchBrokerSeriesForEngine.
	Topology Topology
	// Names is the per-session naming value passed into Topology.EngineSeries.
	Names BenchNames
```

- [ ] **Step 2: Read the current `fetchBrokerSeriesForEngine` body**

Open `benchmarking/aws/runner/matrix.go` lines 277-310. It downloads the per-engine broker dump into `body`, then calls `ParseTopicSeries(body)` and `AttributeByEngine(series, m.SessionID, m.ScenarioConnector)`, then returns `byEngine[engine]`.

- [ ] **Step 3: Replace the parse/attribute block with the topology call**

Replace the section that currently reads (around lines 294-305):

```go
	series, err := ParseTopicSeries(body)
	...
	byEngine, err := AttributeByEngine(series, m.SessionID, m.ScenarioConnector)
	...
	return byEngine[engine]
```

with a call through the topology. The exact surrounding lines (error logging, the `m.ScenarioConnector == ""` guard, the `body` variable's type) must be preserved; only swap the parse+attribute for:

```go
	if m.Topology == nil {
		fmt.Fprintf(stdout, "[bench] no Topology configured; broker attribution skipped\n")
		return nil
	}
	pts, err := m.Topology.EngineSeries(MetricInputs{Body: body, Names: m.Names}, engine)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] EngineSeries(%s) failed: %v\n", engine, err)
		return nil
	}
	return pts
```

> Note: `body` is whatever reader/bytes the existing code already produced from the S3 download. If it is `[]byte`, wrap it as `bytes.NewReader(body)` in the `MetricInputs` literal and ensure `bytes` is imported. If it is already an `io.Reader`, pass it directly. Match the existing type.

- [ ] **Step 4: Build the package**

Run: `go build ./benchmarking/aws/runner/`
Expected: success — all references (`Topology`, `Names`, `renderPipelineConfig`, `runSeeder`) now resolve.

- [ ] **Step 5: Run the full package suite (regression net)**

Run: `go test ./benchmarking/aws/runner/`
Expected: PASS. This is the core proof that the refactor preserved CDC behavior.

- [ ] **Step 6: Vet + format**

Run: `gofmt -l benchmarking/aws/runner/ && go vet ./benchmarking/aws/runner/`
Expected: no files listed by `gofmt`; `go vet` clean.

- [ ] **Step 7: Commit**

```bash
git add benchmarking/aws/runner/main.go benchmarking/aws/runner/matrix.go
git commit -m "refactor(bench): route runBench + MatrixRunner through Topology

runBench selects a Topology by scenario direction and threads it
(plus BenchNames) into pipeline render, seed, workload, reset, and
the broker-series metric. Source path behavior unchanged.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: End-to-end regression check (no AWS spend)

**Files:** none (verification only)

- [ ] **Step 1: Validate an existing CDC scenario through the full load path**

Run: `task aws:validate scenario=postgres/orders-cdc`
Expected: `scenario postgres-orders-cdc OK (...)` — proves `LoadScenario` → `applyDirectionDefault` → `Validate` still accepts the unchanged postgres scenario.

- [ ] **Step 2: Validate the mysql scenario too**

Run: `task aws:validate scenario=mysql/orders-cdc`
Expected: OK.

- [ ] **Step 3: Run the whole runner suite one final time**

Run: `go test ./benchmarking/aws/runner/`
Expected: PASS.

- [ ] **Step 4: Confirm the diff is wiring-only**

Run: `git diff --stat main...HEAD -- benchmarking/aws/runner/`
Expected: changes confined to `scenario.go`, `main.go`, `matrix.go`, the new `topology*.go` files, and the two test files. `scripts.go` and `brokermetrics.go` unchanged (they're still called by `sourceTopology`).

---

## Self-Review

**Spec coverage (Plan 1 scope = the `Topology` abstraction + `BenchNames`, source path only):**
- `Direction` field defaulting to source → Task 1. ✓
- `Topology` interface (6→5 methods; `Validate` deferred, see note) → Task 3. ✓
- `BenchNames` single-source-of-truth → Task 2 (carries SessionID+Connector; full call-site centralization is incremental — Plan 2 extends it). ✓
- `sourceTopology` as a refactor-not-rewrite → Task 4. ✓
- Output-shape invariant (`[]TopicPoint`) → `EngineSeries` returns `[]TopicPoint`; downstream untouched. ✓
- Regression net (existing CDC tests pass) → Tasks 4/6/7. ✓

**Deviation from spec, intentional:** the spec's `Topology.Validate(s)` method is omitted in Plan 1. The source direction's only validation (engineSpec existence) already lives in `Scenario.Validate` and is pinned by an existing test; adding a `Validate` interface method now would either duplicate that check or force moving it (risking the pinned error string). Plan 2 introduces `Topology.Validate` when `sinkTopology` needs direction-specific checks, and migrates the source check then. Recorded here so Plan 2 picks it up.

**Placeholder scan:** none — every code step shows complete code. The two "Note" callouts (prom.go marker format in Task 4; `body` type in Task 6) are verification instructions against real code, not deferred work.

**Type consistency:** `Direction`, `DirectionSource`, `DirectionSink`, `BenchNames`, `newBenchNames`, `ConnectTopic`, `KCTopicPrefix`, `MetricInputs{Body, Names}`, `Topology` (5 methods), `topologyFor`, `sourceTopology` are used identically across Tasks 1-6. `MatrixRunner.Topology`/`Names` added in Task 6 and referenced in Task 5's struct literal (call out the cross-task dependency: do Tasks 5+6 together before building).

---

## Plan 2 preview (not yet written)

Plan 2 — `Iceberg sink` — will add: `sinkTopology` + `sinkSpecs["iceberg"]`, the `Topology.Validate` method, `json-orders` seeder, `icebergmetrics.go` (poller-dump parser → `EngineSeries`), `kcConnectorSpecs["iceberg"]` (`kcSink`), the `terraform/modules/glue-iceberg` + `stacks/iceberg` stack, the `iceberg-kafka-connect` plugin install in `runner-user-data.tftpl`, and the `scenarios/iceberg/orders-sink.yaml` + `-smoke.yaml`. It will be written once Plan 1 lands (and against the live `internal/impl/iceberg/output_iceberg.go` field names + the Apache Iceberg KC sink config reference).

### Carryover from Plan 1 final review — source-assumptions still OUTSIDE the `Topology` interface

The Plan 1 interface boundary (pipeline + scripts + per-engine series *parsing*) is sound, but several source-shaped decisions live in the *orchestration* layer and will NOT be fixed automatically by returning a `sinkTopology` from `topologyFor`. Plan 2 must address these explicitly:

1. **`MatrixRunner.Run` hard-codes Connect-vs-KC engine semantics** (`matrix.go` ~175-197): `engine == "connect"` parses Connect's rolling-stats log; `engine == "kafka_connect"` derives the Summary from broker bytes via `SummariseTopicPoints`. A sink's throughput is the Iceberg snapshot poll — there may be no broker-produce series at all. Decide whether Summary-derivation moves behind the topology.
2. **Early-abort guard is `engine == "connect"` + `len(samples) == 0`** (`matrix.go` ~220). A sink whose throughput comes from snapshot polling (not parsed log samples) may never trip or may trip spuriously. Decide whether the abort signal comes from the topology.
3. **The metric *fetch* (not just parse) is source-shaped.** `fetchBrokerSeriesForEngine` always downloads `redpanda-N-<suffix>.txt` from S3 (`matrix.go` ~288) and the suffix mapping (`connect`→`redpanda-N-connect.txt`, `kafka_connect`→`redpanda-N-kc.txt`) is decided in `MatrixRunner`, not the topology. A sink's metric artifact is the Iceberg poll dump (`iceberg-N-<engine>.txt`). Plan 2 needs a topology-supplied artifact key OR a generalized fetch — `EngineSeries` parsing alone isn't enough.
4. **`buildKCRenderInputs` + KC connector rendering** (`main.go` ~254, `kcconnectors.go`) is Debezium/source-specific and invoked directly from `runBench`, not via the topology. For a sink KC connector, this path needs its own seam; if sinks skip KC, `runBench` must branch on `DirectionSink`.
5. **`Topology.Validate`** was deferred in Plan 1 (the source engineSpec check stayed in `Scenario.Validate`). Plan 2 introduces it for sink-specific validation and migrates the source check.

Minor Plan-1 leftovers Plan 2 should also tidy: the `BenchNames.ConnectTopic()`/`KCTopicPrefix()` helpers exist but no production site routes through them yet (the inline `fmt.Sprintf` calls in `sourceTopology.Pipeline` and `buildKCRenderInputs` remain); and the seed S3 key is computed in two places (`runSeeder` for upload vs `sourceTopology.SeedScript` for the script body) — a `BenchNames.SeedKey()` could own it.
