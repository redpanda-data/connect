# Kafka Connect head-to-head — Plan 2: Engine swap for source connectors

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `task aws:bench` run BOTH Redpanda Connect and Kafka Connect for each vCPU sweep point, against the same workload, on the same hardware. Connect's pipeline output flips from `drop` to the `redpanda` first-party output writing to a per-engine Redpanda topic. KC submits a Debezium connector via REST and writes to its own per-engine topic. Each sweep point produces TWO `PointResult` records (one per engine). Plan 3 owns the reporting; Plan 2 produces the raw side-by-side data.

**Architecture:** A new `kcConnectorSpec` registry (mirroring the existing `engineSpec` map) provides per-connector KC equivalents — JSON config template + connector class + plugin requirements. A small REST client submits the connector to the long-lived KC worker. The matrix runner gains an engine-inner loop with `vcpu outer, engine inner`. The KC worker is restarted per sweep point via JVM-direct spawn (taskset + `-Xmx`) so it gets the same CPU + memory budget as Connect. Reset extends to delete topics + DELETE the connector.

**Tech Stack:** Go (existing runner package), Apache Kafka 3.8 Connect distributed mode, Debezium 2.7.3 (Postgres + MySQL), Redpanda 24.x (cluster from Plan 1), `redpanda` output in Connect with top-level `redpanda:` block.

**Spec:** [`docs/superpowers/specs/2026-05-22-kafka-connect-comparison-design.md`](../specs/2026-05-22-kafka-connect-comparison-design.md)
**Predecessor:** [Plan 1 (Redpanda + KC infra)](2026-05-22-kafka-connect-plan-1-infra.md) — must be applied via Task 12 before Plan 2 is run, since Plan 2 needs a real Redpanda cluster + KC worker.

---

## File Structure

**New files:**

- `benchmarking/aws/runner/kcconnectors.go` — `kcConnectorSpec` type, `kcConnectorSpecs` registry, `kcConnectorSpecFor()`, `renderKCConfig()` (template render + per-scenario override merge).
- `benchmarking/aws/runner/kcconnectors_test.go` — registry lookup tests + renderKCConfig template tests.
- `benchmarking/aws/runner/kcrest.go` — minimal KC REST client: `SubmitConnector`, `WaitConnectorRunning`, `DeleteConnector`.
- `benchmarking/aws/runner/kcrest_test.go` — `httptest.Server`-driven tests (no live KC required).
- `benchmarking/aws/runner/kcscript.go` — `renderKCBenchScript` (the per-sweep-point shell script for a KC engine pass; differs from Connect's by handling the JVM lifecycle + REST submit).
- `benchmarking/aws/runner/kcscript_test.go` — tests for the rendered script content.

**Modified files:**

- `benchmarking/aws/runner/scenario.go` — keep existing `Scenario`/`engineSpec`. Add `KafkaConnect map[string]any` to `Scenario` as an optional override.
- `benchmarking/aws/runner/render.go` — `PointResult` gains `Engine string` field. `AppendMarkdown` is left unchanged in Plan 2 (it still iterates points by VCPU); Plan 3 reworks the markdown shape to be engine-aware.
- `benchmarking/aws/runner/render_test.go` — fixture updates: existing `PointResult` literals get `Engine: "connect"`.
- `benchmarking/aws/runner/matrix.go` — `SweepPoint` gains `Engine string`. `MatrixRunner.Run` gains `engines []string` parameter; engine-inner loop; per-engine bench script dispatch.
- `benchmarking/aws/runner/matrix_test.go` — new tests for the engine-inner loop + per-engine script dispatch.
- `benchmarking/aws/runner/main.go` — `renderPipelineConfig` switches from `drop` to `redpanda` output + top-level `redpanda:` block. New `--engines` CLI flag (default `"connect,kafka_connect"`). `runBench` parses the flag and passes engines through to `MatrixRunner.Run`.
- `benchmarking/aws/runner/scripts.go` — `combineReset` extends with engine-aware additional reset commands (`kafka-topics.sh --delete` + KC REST `DELETE /connectors/<name>`).
- `benchmarking/aws/runner/scripts_test.go` — tests for the new reset extensions.

**Why this split:**

- `kcconnectors.go` and `kcrest.go` are independent concerns — registry vs network — and small enough to be readable on their own. Keeping them separate also lets `kcrest_test.go` use only the `httptest` machinery without pulling in any registry detail.
- `kcscript.go` is the KC analog of `matrix.go`'s `renderBenchScript`. Putting the KC-specific bash in its own file keeps `matrix.go` focused on the sweep orchestration.
- The `redpanda` output change in `renderPipelineConfig` is a single-function edit — no need to pull it into its own file.

---

## Task ordering

Plan 2 has three phases:

- **Phase A (Tasks 1–4):** Data model + pipeline output change. Pure refactor — bench still works after each task, single-engine, just with a `redpanda` output and an `Engine` field plumbed through.
- **Phase B (Tasks 5–9):** KC machinery — registry, REST client, bench-script renderer. All TDD; nothing wired into the orchestrator yet.
- **Phase C (Tasks 10–13):** Wire it up. Engine loop in matrix runner, reset extensions, CLI flag, end-to-end.
- **Phase D (Tasks 14–15):** README + operator-driven smoke test.

After Phase A: `task aws:bench` runs unchanged; `output: redpanda` produces the same throughput as `output: drop` would have (plus broker traffic; Plan 1's scraper now captures real bytes).
After Phase B: registry + REST helpers tested in isolation; no behavior change at the orchestrator layer.
After Phase C: `task aws:bench` produces 2× the points per scenario, half for each engine.
After Phase D: documented + smoke-tested.

---

### Task 1: Add `Engine` field to `PointResult` and `SweepPoint`

**Files:**
- Modify: `benchmarking/aws/runner/render.go`
- Modify: `benchmarking/aws/runner/render_test.go`
- Modify: `benchmarking/aws/runner/matrix.go`

- [ ] **Step 1: Add `Engine string` to `PointResult` in `render.go`**

Find:
```go
type PointResult struct {
	VCPU      int         `json:"vcpu"`
	Samples   []Sample    `json:"samples"`
	Summary   Summary     `json:"summary"`
	Anomalies []Anomaly   `json:"anomalies,omitempty"`
	Prom      []PromPoint `json:"prom,omitempty"`
}
```

Replace with:
```go
type PointResult struct {
	VCPU      int         `json:"vcpu"`
	Engine    string      `json:"engine"`
	Samples   []Sample    `json:"samples"`
	Summary   Summary     `json:"summary"`
	Anomalies []Anomaly   `json:"anomalies,omitempty"`
	Prom      []PromPoint `json:"prom,omitempty"`
}
```

- [ ] **Step 2: Add `Engine string` to `SweepPoint` in `matrix.go`**

Find:
```go
type SweepPoint struct {
	VCPU      int
	Samples   []Sample
	Summary   Summary
	Anomalies []Anomaly
	Prom      []PromPoint
}
```

Replace with:
```go
type SweepPoint struct {
	VCPU      int
	Engine    string
	Samples   []Sample
	Summary   Summary
	Anomalies []Anomaly
	Prom      []PromPoint
}
```

- [ ] **Step 3: Update render_test.go fixtures**

Find every `PointResult{...}` literal in `render_test.go` (run `grep -n "PointResult{" benchmarking/aws/runner/render_test.go` to locate them). Add `Engine: "connect"` to each literal. For example, if a test has:

```go
points := []PointResult{
    {VCPU: 1, Samples: ..., Summary: ...},
    {VCPU: 2, Samples: ..., Summary: ...},
}
```

Change to:

```go
points := []PointResult{
    {VCPU: 1, Engine: "connect", Samples: ..., Summary: ...},
    {VCPU: 2, Engine: "connect", Samples: ..., Summary: ...},
}
```

(Plan 2 keeps single-engine semantics throughout these existing tests; Plan 3 adds the dual-engine table fixture.)

- [ ] **Step 4: Build + tests**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go build ./benchmarking/aws/runner/...
go test ./benchmarking/aws/runner/...
```

Expected: clean build, all tests pass. The JSON output of `WriteResultJSON` now includes `"engine": "connect"` per point.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/render.go benchmarking/aws/runner/matrix.go benchmarking/aws/runner/render_test.go
git commit -m "feat(bench/aws): add Engine field to PointResult and SweepPoint"
```

---

### Task 2: Switch Connect's pipeline output from `drop` to `redpanda`

**Files:**
- Modify: `benchmarking/aws/runner/main.go` (function `renderPipelineConfig` around line 471)

- [ ] **Step 1: Locate the current `renderPipelineConfig`**

```bash
sed -n '470,510p' /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/runner/main.go
```

Current body builds `cfg` with `output.drop`. The new body builds `output.redpanda` and adds a top-level `redpanda:` block.

- [ ] **Step 2: Rewrite the function**

Replace the entire `renderPipelineConfig` function (lines ~470-507 — confirm the exact range before editing) with:

```go
func renderPipelineConfig(s *Scenario, outs map[string]string) (string, error) {
	// Per-engine output topic so broker-side metrics attribute cleanly.
	// SessionID is substituted by substitutePlaceholders below.
	topic := fmt.Sprintf("bench_${BENCH_SESSION_ID}_%s_connect", s.Connector)

	cfg := map[string]any{
		"http": map[string]any{"debug_endpoints": true},
		"redpanda": map[string]any{
			"seed_brokers": []string{"${REDPANDA_BROKER_ENDPOINTS}"},
		},
		"input": s.Pipeline["input"],
		"output": map[string]any{
			"processors": []any{
				map[string]any{
					"benchmark": map[string]any{"interval": "1s", "count_bytes": true},
				},
			},
			"redpanda": map[string]any{
				"topic": topic,
			},
		},
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

- [ ] **Step 3: Ensure `BENCH_SESSION_ID` and `REDPANDA_BROKER_ENDPOINTS` reach the `outs` map**

In `main.go`, find where `sharedOuts` is constructed (around line 147-151). Confirm it already includes `bench_session_id` from the runner (look for `"bench_session_id"`). It does.

Then `substitutePlaceholders` reads `outs` keys uppercased with `${}` interpolation — find the function around line 510 and confirm. (If `substitutePlaceholders` only substitutes the literal `outs` keys: read its body; it should be replacing `${BENCH_SESSION_ID}` with the value at `outs["bench_session_id"]`. If the case-folding doesn't match, fix the new template strings to use whatever casing it does — likely lowercase.)

Run this check:
```bash
sed -n '509,520p' /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/runner/main.go
```

If the function does a `strings.ReplaceAll(in, "${"+k+"}", v)` (no case conversion), then the placeholders must be `${bench_session_id}` and `${redpanda_broker_endpoints}`, lowercase. Update the new `renderPipelineConfig` body accordingly.

- [ ] **Step 4: Build + run existing tests**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go build ./benchmarking/aws/runner/...
go test ./benchmarking/aws/runner/...
```

Expected: clean. Existing tests don't introspect the YAML output — they exercise renderers separately. No regressions.

- [ ] **Step 5: Manually verify the rendered YAML looks right**

```bash
cat > /tmp/scen.yaml <<'EOF'
name: postgres-test
connector: postgres_cdc
stack: postgres
infra:
  source: {}
  runner:
    instance_type: c8g.4xlarge
dataset:
  initial_rows: 0
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows
workload:
  write_rate_per_sec: 150000
  duration: 15m
  warmup: 2m
pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
      tls:
        skip_cert_verify: true
      stream_snapshot: false
      schema: public
      tables: [orders]
      slot_name: bench_slot
matrix:
  cpu_points: [1]
reset: []
EOF

cd /Users/prakhar.garg/Documents/connect_prakhar
cat > /tmp/renderchk.go <<'EOF'
package main

import (
	"fmt"
	"os"
)

func main() {
	s, err := LoadScenario("/tmp/scen.yaml")
	if err != nil { panic(err) }
	outs := map[string]string{
		"bench_session_id":          "sess-abc",
		"postgres_dsn":              "postgres://...",
		"redpanda_broker_endpoints": "10.42.10.10:9092,10.42.11.10:9092,10.42.10.11:9092",
	}
	path, err := renderPipelineConfig(s, outs)
	if err != nil { panic(err) }
	defer os.Remove(path)
	raw, _ := os.ReadFile(path)
	fmt.Print(string(raw))
}
EOF
# Manual inspection — not committed
```

Actually, **skip the manual check script** — it's awkward to wire as a one-off. The byte-shape regression is caught by Task 3's tests.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): Connect pipeline writes to redpanda output, not drop"
```

---

### Task 3: Add `renderPipelineConfig` regression test

**Files:**
- Create: `benchmarking/aws/runner/main_render_test.go` (new file, separate from existing main tests if any)

- [ ] **Step 1: Write the test**

Create `benchmarking/aws/runner/main_render_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"os"
	"strings"
	"testing"
)

func TestRenderPipelineConfig_PostgresCDC(t *testing.T) {
	s := &Scenario{
		Name:      "postgres-test",
		Connector: "postgres_cdc",
		Stack:     "postgres",
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{
					"dsn":             "${POSTGRES_DSN}",
					"stream_snapshot": false,
					"tables":          []string{"orders"},
					"slot_name":       "bench_slot",
				},
			},
		},
	}
	outs := map[string]string{
		"bench_session_id":          "sess-abc",
		"postgres_dsn":              "postgres://user:pw@host/db",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}

	path, err := renderPipelineConfig(s, outs)
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rendered config: %v", err)
	}
	body := string(raw)

	if !strings.Contains(body, "seed_brokers:") {
		t.Errorf("expected top-level redpanda.seed_brokers in rendered config; got:\n%s", body)
	}
	if !strings.Contains(body, "10.42.10.10:9092") {
		t.Errorf("expected substituted broker endpoint; got:\n%s", body)
	}
	if !strings.Contains(body, "topic: bench_sess-abc_postgres_cdc_connect") {
		t.Errorf("expected per-engine topic name; got:\n%s", body)
	}
	if !strings.Contains(body, "postgres_cdc:") {
		t.Errorf("expected postgres_cdc input block; got:\n%s", body)
	}
	if !strings.Contains(body, "benchmark:") {
		t.Errorf("expected benchmark processor; got:\n%s", body)
	}
	if strings.Contains(body, "drop:") {
		t.Errorf("output should be redpanda not drop; got:\n%s", body)
	}
}

func TestRenderPipelineConfig_ThreadsCacheResources(t *testing.T) {
	s := &Scenario{
		Name:      "mysql-test",
		Connector: "mysql_cdc",
		Stack:     "mysql",
		Pipeline: map[string]any{
			"cache_resources": []any{
				map[string]any{"label": "bench_checkpoint", "memory": map[string]any{}},
			},
			"input": map[string]any{
				"mysql_cdc": map[string]any{
					"dsn": "${MYSQL_DSN}",
				},
			},
		},
	}
	outs := map[string]string{
		"bench_session_id":          "sess-xyz",
		"mysql_dsn":                 "user:pw@tcp(host)/db",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}

	path, err := renderPipelineConfig(s, outs)
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })

	raw, _ := os.ReadFile(path)
	body := string(raw)
	if !strings.Contains(body, "cache_resources:") {
		t.Errorf("expected cache_resources to be threaded through; got:\n%s", body)
	}
	if !strings.Contains(body, "bench_checkpoint") {
		t.Errorf("expected cache_resources content; got:\n%s", body)
	}
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go test ./benchmarking/aws/runner -run TestRenderPipelineConfig -v
```

Expected: both tests pass against the implementation from Task 2.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/runner/main_render_test.go
git commit -m "test(bench/aws): regression tests for renderPipelineConfig (redpanda output, cache_resources)"
```

---

### Task 4: Add optional `KafkaConnect` override block to `Scenario`

**Files:**
- Modify: `benchmarking/aws/runner/scenario.go`
- Modify: `benchmarking/aws/runner/scenario_test.go`

- [ ] **Step 1: Add field to `Scenario`**

In `scenario.go`, find the `Scenario` struct (around line 44) and add `KafkaConnect` at the end:

```go
type Scenario struct {
	Name        string         `yaml:"name"`
	Description string         `yaml:"description"`
	Connector   string         `yaml:"connector"`
	Stack       string         `yaml:"stack"`
	Infra       InfraSpec      `yaml:"infra"`
	Dataset     DatasetSpec    `yaml:"dataset"`
	Workload    *WorkloadSpec  `yaml:"workload,omitempty"`
	Pipeline    map[string]any `yaml:"pipeline"`
	Matrix      MatrixSpec     `yaml:"matrix"`
	Reset       []ResetStep    `yaml:"reset"`
	// KafkaConnect is an optional override map applied on top of the
	// kcConnectorSpec registry entry's PropsTemplate at render time. The
	// fields here are shallow-merged into the resulting KC connector config
	// JSON. Use this to tune e.g. snapshot.mode without editing the registry.
	KafkaConnect map[string]any `yaml:"kafka_connect,omitempty"`
}
```

- [ ] **Step 2: Write a test**

Append to `benchmarking/aws/runner/scenario_test.go`:

```go
func TestLoadScenario_KafkaConnectOverride(t *testing.T) {
	const yamlBody = `
name: test
connector: postgres_cdc
stack: postgres
infra:
  source: {}
  runner:
    instance_type: c8g.4xlarge
dataset:
  initial_rows: 0
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows
workload:
  write_rate_per_sec: 150000
  duration: 15m
  warmup: 2m
pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
matrix:
  cpu_points: [1]
reset: []
kafka_connect:
  config:
    snapshot.mode: never
    decimal.handling.mode: string
`
	tmp, _ := os.CreateTemp("", "scen-*.yaml")
	t.Cleanup(func() { os.Remove(tmp.Name()) })
	tmp.WriteString(yamlBody)
	tmp.Close()

	s, err := LoadScenario(tmp.Name())
	if err != nil {
		t.Fatalf("LoadScenario: %v", err)
	}
	if s.KafkaConnect == nil {
		t.Fatalf("KafkaConnect field should be populated")
	}
	cfg, ok := s.KafkaConnect["config"].(map[string]any)
	if !ok {
		t.Fatalf("expected kafka_connect.config map; got %T", s.KafkaConnect["config"])
	}
	if cfg["snapshot.mode"] != "never" {
		t.Errorf("snapshot.mode = %v, want never", cfg["snapshot.mode"])
	}
}

func TestLoadScenario_KafkaConnectOptional(t *testing.T) {
	const yamlBody = `
name: test
connector: postgres_cdc
stack: postgres
infra:
  source: {}
  runner:
    instance_type: c8g.4xlarge
dataset:
  initial_rows: 0
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows
workload:
  write_rate_per_sec: 150000
  duration: 15m
  warmup: 2m
pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
matrix:
  cpu_points: [1]
reset: []
`
	tmp, _ := os.CreateTemp("", "scen-*.yaml")
	t.Cleanup(func() { os.Remove(tmp.Name()) })
	tmp.WriteString(yamlBody)
	tmp.Close()

	s, err := LoadScenario(tmp.Name())
	if err != nil {
		t.Fatalf("LoadScenario: %v", err)
	}
	if s.KafkaConnect != nil {
		t.Errorf("KafkaConnect should be nil when omitted; got %v", s.KafkaConnect)
	}
}
```

- [ ] **Step 3: Run tests**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go test ./benchmarking/aws/runner -run TestLoadScenario_KafkaConnect -v
```

Expected: both tests pass.

- [ ] **Step 4: Run full suite**

```bash
go test ./benchmarking/aws/runner/...
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/scenario.go benchmarking/aws/runner/scenario_test.go
git commit -m "feat(bench/aws): optional kafka_connect override block in scenario YAML"
```

---

### Task 5: Create `kcConnectorSpec` type and registry skeleton

**Files:**
- Create: `benchmarking/aws/runner/kcconnectors.go`
- Create: `benchmarking/aws/runner/kcconnectors_test.go`

- [ ] **Step 1: Write the failing test FIRST**

Create `benchmarking/aws/runner/kcconnectors_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

func TestKCConnectorSpecFor_Unknown(t *testing.T) {
	_, ok := kcConnectorSpecFor("does_not_exist")
	if ok {
		t.Error("expected lookup of unknown connector to return ok=false")
	}
}

func TestKCConnectorSpecFor_PostgresPlaceholder(t *testing.T) {
	// Registry will be filled in later tasks. This test just confirms
	// the lookup function compiles and returns ok=false on an empty registry.
	_, ok := kcConnectorSpecFor("postgres_cdc")
	_ = ok // not asserting yet; future tasks add entries.
}
```

- [ ] **Step 2: Run — expect compile failure (no kcConnectorSpec yet)**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go test ./benchmarking/aws/runner -run TestKCConnectorSpec -v
```

Expected: build fails with `undefined: kcConnectorSpecFor` (or similar).

- [ ] **Step 3: Create the skeleton**

Create `benchmarking/aws/runner/kcconnectors.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

// kcDirection captures whether the connector reads from or writes to the
// shared Redpanda cluster. The matrix runner uses this to attribute the
// broker-side throughput metric to produce-side (sources) or consume-side
// (sinks) traffic. Plan 2 only ships sources.
type kcDirection int

const (
	kcSource kcDirection = iota
	kcSink
)

// kcConnectorSpec describes the Kafka Connect counterpart of a Redpanda
// Connect connector. Each entry pins the connector class, the JSON config
// template (which can reference scenario fields + TF outputs via Go
// text/template syntax), and any plugin globs that should exist on the
// runner host before the connector is submitted.
//
// To add a new connector to the comparison framework, add one entry to
// kcConnectorSpecs below. Touch no other files.
type kcConnectorSpec struct {
	Class           string
	PropsTemplate   string
	Direction       kcDirection
	RequiredPlugins []string
}

// kcConnectorSpecs is the registry of KC counterparts keyed by the Redpanda
// Connect connector name (the same key used in engineSpecs).
var kcConnectorSpecs = map[string]kcConnectorSpec{}

func kcConnectorSpecFor(connector string) (kcConnectorSpec, bool) {
	es, ok := kcConnectorSpecs[connector]
	return es, ok
}
```

- [ ] **Step 4: Run tests — both should pass now**

```bash
go test ./benchmarking/aws/runner -run TestKCConnectorSpec -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git commit -m "feat(bench/aws/kc): kcConnectorSpec type + empty registry"
```

---

### Task 6: Registry entry for `postgres_cdc` (Debezium Postgres)

**Files:**
- Modify: `benchmarking/aws/runner/kcconnectors.go`
- Modify: `benchmarking/aws/runner/kcconnectors_test.go`

- [ ] **Step 1: Write failing tests**

Append to `benchmarking/aws/runner/kcconnectors_test.go`:

```go
func TestKCConnectorSpecFor_PostgresCDC(t *testing.T) {
	es, ok := kcConnectorSpecFor("postgres_cdc")
	if !ok {
		t.Fatal("postgres_cdc should be registered")
	}
	if es.Class != "io.debezium.connector.postgresql.PostgresConnector" {
		t.Errorf("Class = %q, want Debezium Postgres class", es.Class)
	}
	if es.Direction != kcSource {
		t.Errorf("Direction should be kcSource")
	}
	if es.PropsTemplate == "" {
		t.Error("PropsTemplate should not be empty")
	}
	// The template should reference at least the DSN substitution and the
	// table list.
	if !strings.Contains(es.PropsTemplate, "{{.HostPort}}") &&
		!strings.Contains(es.PropsTemplate, "{{.Host}}") {
		t.Errorf("template should interpolate host info; got:\n%s", es.PropsTemplate)
	}
	if !strings.Contains(es.PropsTemplate, "{{.Tables}}") {
		t.Errorf("template should interpolate table list; got:\n%s", es.PropsTemplate)
	}
	if len(es.RequiredPlugins) == 0 {
		t.Error("RequiredPlugins should list at least debezium-connector-postgres glob")
	}
}
```

Add `"strings"` to the import block at the top of `kcconnectors_test.go` if it isn't already.

- [ ] **Step 2: Run — should fail**

```bash
go test ./benchmarking/aws/runner -run TestKCConnectorSpecFor_PostgresCDC -v
```

Expected: FAIL with "postgres_cdc should be registered".

- [ ] **Step 3: Add the registry entry**

In `kcconnectors.go`, replace the empty `kcConnectorSpecs` map with:

```go
var kcConnectorSpecs = map[string]kcConnectorSpec{
	"postgres_cdc": {
		Class:     "io.debezium.connector.postgresql.PostgresConnector",
		Direction: kcSource,
		// PropsTemplate is rendered via Go text/template. The render
		// inputs are documented next to renderKCConfig (Task 8). The
		// JSON shape here is what Debezium 2.7.x expects from the KC
		// REST PUT /connectors/<name>/config endpoint.
		PropsTemplate: `{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "{{.Host}}",
  "database.port": "{{.Port}}",
  "database.user": "{{.User}}",
  "database.password": "{{.Password}}",
  "database.dbname": "{{.Database}}",
  "topic.prefix": "{{.TopicPrefix}}",
  "table.include.list": "{{.SchemaTables}}",
  "plugin.name": "pgoutput",
  "slot.name": "kc_bench_slot",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "never",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-postgres*"},
	},
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestKCConnectorSpecFor_PostgresCDC -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git commit -m "feat(bench/aws/kc): registry entry for postgres_cdc (Debezium Postgres)"
```

---

### Task 7: Registry entry for `mysql_cdc` (Debezium MySQL)

**Files:**
- Modify: `benchmarking/aws/runner/kcconnectors.go`
- Modify: `benchmarking/aws/runner/kcconnectors_test.go`

- [ ] **Step 1: Write failing test**

Append to `kcconnectors_test.go`:

```go
func TestKCConnectorSpecFor_MySQLCDC(t *testing.T) {
	es, ok := kcConnectorSpecFor("mysql_cdc")
	if !ok {
		t.Fatal("mysql_cdc should be registered")
	}
	if es.Class != "io.debezium.connector.mysql.MySqlConnector" {
		t.Errorf("Class = %q, want Debezium MySQL class", es.Class)
	}
	if es.Direction != kcSource {
		t.Errorf("Direction should be kcSource")
	}
	if !strings.Contains(es.PropsTemplate, "database.server.id") {
		t.Errorf("MySQL Debezium config must include server.id; got:\n%s", es.PropsTemplate)
	}
	if !strings.Contains(es.PropsTemplate, "{{.Host}}") {
		t.Errorf("template should interpolate host; got:\n%s", es.PropsTemplate)
	}
}
```

- [ ] **Step 2: Run — should fail**

```bash
go test ./benchmarking/aws/runner -run TestKCConnectorSpecFor_MySQLCDC -v
```

Expected: FAIL with "mysql_cdc should be registered".

- [ ] **Step 3: Add the registry entry**

In `kcconnectors.go`'s `kcConnectorSpecs` map, add (alongside `postgres_cdc`):

```go
	"mysql_cdc": {
		Class:     "io.debezium.connector.mysql.MySqlConnector",
		Direction: kcSource,
		PropsTemplate: `{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "tasks.max": "1",
  "database.hostname": "{{.Host}}",
  "database.port": "{{.Port}}",
  "database.user": "{{.User}}",
  "database.password": "{{.Password}}",
  "database.server.id": "184054",
  "database.include.list": "{{.Database}}",
  "table.include.list": "{{.SchemaTables}}",
  "topic.prefix": "{{.TopicPrefix}}",
  "schema.history.internal.kafka.bootstrap.servers": "{{.BootstrapServers}}",
  "schema.history.internal.kafka.topic": "_kc_schema_history_{{.TopicPrefix}}",
  "snapshot.mode": "never",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false"
}`,
		RequiredPlugins: []string{"debezium-connector-mysql*"},
	},
```

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestKCConnectorSpecFor -v
```

Expected: all 4 tests pass (Unknown, Placeholder, Postgres, MySQL).

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git commit -m "feat(bench/aws/kc): registry entry for mysql_cdc (Debezium MySQL)"
```

---

### Task 8: `renderKCConfig` — render template + apply scenario override

**Files:**
- Modify: `benchmarking/aws/runner/kcconnectors.go`
- Modify: `benchmarking/aws/runner/kcconnectors_test.go`

- [ ] **Step 1: Write failing tests**

Append to `kcconnectors_test.go`:

```go
func TestRenderKCConfig_PostgresBasic(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{
					"tables": []any{"orders"},
					"schema": "public",
				},
			},
		},
	}
	inputs := kcRenderInputs{
		Host:             "rds.example.com",
		Port:             "5432",
		User:             "bench",
		Password:         "s3cret",
		Database:         "benchdb",
		Tables:           []string{"orders"},
		SchemaTables:     "public.orders",
		TopicPrefix:      "bench_sess123_postgres_cdc_kc",
		BootstrapServers: "10.42.10.10:9092",
	}
	cfg, err := renderKCConfig(s, inputs)
	if err != nil {
		t.Fatalf("renderKCConfig: %v", err)
	}
	if cfg["database.hostname"] != "rds.example.com" {
		t.Errorf("hostname = %v, want rds.example.com", cfg["database.hostname"])
	}
	if cfg["database.port"] != "5432" {
		t.Errorf("port = %v, want 5432", cfg["database.port"])
	}
	if cfg["table.include.list"] != "public.orders" {
		t.Errorf("table.include.list = %v, want public.orders", cfg["table.include.list"])
	}
	if cfg["topic.prefix"] != "bench_sess123_postgres_cdc_kc" {
		t.Errorf("topic.prefix = %v", cfg["topic.prefix"])
	}
	if cfg["snapshot.mode"] != "never" {
		t.Errorf("snapshot.mode should default to never; got %v", cfg["snapshot.mode"])
	}
}

func TestRenderKCConfig_ScenarioOverride(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{
					"tables": []any{"orders"},
					"schema": "public",
				},
			},
		},
		KafkaConnect: map[string]any{
			"config": map[string]any{
				"snapshot.mode":           "initial",
				"decimal.handling.mode":   "string",
			},
		},
	}
	inputs := kcRenderInputs{
		Host: "rds.example.com", Port: "5432",
		User: "bench", Password: "s3cret", Database: "benchdb",
		Tables: []string{"orders"}, SchemaTables: "public.orders",
		TopicPrefix: "bench_sess123_postgres_cdc_kc",
		BootstrapServers: "10.42.10.10:9092",
	}
	cfg, err := renderKCConfig(s, inputs)
	if err != nil {
		t.Fatalf("renderKCConfig: %v", err)
	}
	if cfg["snapshot.mode"] != "initial" {
		t.Errorf("override should win; got snapshot.mode = %v", cfg["snapshot.mode"])
	}
	if cfg["decimal.handling.mode"] != "string" {
		t.Errorf("override should win; got decimal.handling.mode = %v", cfg["decimal.handling.mode"])
	}
	// Non-overridden fields preserved from the registry template:
	if cfg["database.hostname"] != "rds.example.com" {
		t.Errorf("base template field should survive override; got %v", cfg["database.hostname"])
	}
}

func TestRenderKCConfig_UnknownConnector(t *testing.T) {
	s := &Scenario{Connector: "does_not_exist"}
	_, err := renderKCConfig(s, kcRenderInputs{})
	if err == nil {
		t.Error("expected error for unknown connector")
	}
}
```

- [ ] **Step 2: Run — should fail (function doesn't exist)**

```bash
go test ./benchmarking/aws/runner -run TestRenderKCConfig -v
```

Expected: build error (`undefined: renderKCConfig`).

- [ ] **Step 3: Implement `renderKCConfig` and `kcRenderInputs`**

Append to `kcconnectors.go`:

```go
import (
	// existing imports if any go here. Add as needed.
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
)

// kcRenderInputs carries the values interpolated into a kcConnectorSpec's
// PropsTemplate. Populated by the orchestrator from TF outputs + scenario.
type kcRenderInputs struct {
	// Database connection
	Host     string
	Port     string
	User     string
	Password string
	Database string

	// Tables to capture (formatted differently per engine in SchemaTables)
	Tables       []string
	SchemaTables string // engine-specific, e.g. "public.orders" for PG, "benchdb.orders" for MySQL

	// Output topic prefix for Debezium (Debezium prepends to each table topic)
	TopicPrefix string

	// Kafka bootstrap.servers for the internal schema-history topic (MySQL)
	BootstrapServers string
}

// renderKCConfig produces the JSON config map ready to POST to the KC REST
// API. It looks up the connector's PropsTemplate, renders it with the given
// inputs, then merges any per-scenario `kafka_connect.config` overrides on
// top.
func renderKCConfig(s *Scenario, in kcRenderInputs) (map[string]any, error) {
	spec, ok := kcConnectorSpecFor(s.Connector)
	if !ok {
		return nil, fmt.Errorf("no kcConnectorSpec registered for connector %q", s.Connector)
	}

	tmpl, err := template.New("kc").Parse(spec.PropsTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse template for %q: %w", s.Connector, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, in); err != nil {
		return nil, fmt.Errorf("render template for %q: %w", s.Connector, err)
	}

	var cfg map[string]any
	dec := json.NewDecoder(strings.NewReader(buf.String()))
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode rendered JSON for %q: %w; body:\n%s", s.Connector, err, buf.String())
	}

	// Shallow-merge scenario's `kafka_connect.config` over the base.
	if s.KafkaConnect != nil {
		if over, ok := s.KafkaConnect["config"].(map[string]any); ok {
			for k, v := range over {
				cfg[k] = v
			}
		}
	}

	return cfg, nil
}
```

- [ ] **Step 4: Run tests — should pass**

```bash
go test ./benchmarking/aws/runner -run TestRenderKCConfig -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Full suite**

```bash
go test ./benchmarking/aws/runner/...
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/kcconnectors.go benchmarking/aws/runner/kcconnectors_test.go
git commit -m "feat(bench/aws/kc): renderKCConfig with template render + scenario override merge"
```

---

### Task 9: KC REST client (SubmitConnector, WaitConnectorRunning, DeleteConnector)

**Files:**
- Create: `benchmarking/aws/runner/kcrest.go`
- Create: `benchmarking/aws/runner/kcrest_test.go`

- [ ] **Step 1: Write failing tests with httptest**

Create `benchmarking/aws/runner/kcrest_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmitConnector_PUT(t *testing.T) {
	var gotMethod string
	var gotURL string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotURL = r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"name":"bench_x","config":{}}`))
	}))
	defer srv.Close()

	cfg := map[string]any{"connector.class": "x"}
	if err := SubmitConnector(context.Background(), srv.URL, "bench_x", cfg); err != nil {
		t.Fatalf("SubmitConnector: %v", err)
	}
	if gotMethod != "PUT" {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	if gotURL != "/connectors/bench_x/config" {
		t.Errorf("URL = %s, want /connectors/bench_x/config", gotURL)
	}
	if gotBody["connector.class"] != "x" {
		t.Errorf("body class = %v", gotBody["connector.class"])
	}
}

func TestWaitConnectorRunning_HappyPath(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			// First call: not yet
			w.Write([]byte(`{"connector":{"state":"UNASSIGNED"},"tasks":[]}`))
			return
		}
		// Subsequent calls: ready
		w.Write([]byte(`{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitConnectorRunning(ctx, srv.URL, "bench_x", 50*time.Millisecond); err != nil {
		t.Fatalf("WaitConnectorRunning: %v", err)
	}
	if atomic.LoadInt32(&calls) < 2 {
		t.Errorf("expected at least 2 status polls; got %d", calls)
	}
}

func TestWaitConnectorRunning_TaskFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"FAILED","trace":"boom"}]}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := WaitConnectorRunning(ctx, srv.URL, "bench_x", 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error when a task is FAILED")
	}
	if !strings.Contains(err.Error(), "FAILED") {
		t.Errorf("error should mention FAILED state; got %v", err)
	}
}

func TestDeleteConnector_404IsOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	// DELETE on a non-existent connector should be a no-op (idempotent reset).
	if err := DeleteConnector(context.Background(), srv.URL, "missing"); err != nil {
		t.Errorf("DeleteConnector on 404 should succeed; got %v", err)
	}
}

func TestDeleteConnector_204IsOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	if err := DeleteConnector(context.Background(), srv.URL, "bench_x"); err != nil {
		t.Errorf("DeleteConnector on 204 should succeed; got %v", err)
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./benchmarking/aws/runner -run TestSubmitConnector -v
```

Expected: build error (`undefined: SubmitConnector` etc.).

- [ ] **Step 3: Implement the REST client**

Create `benchmarking/aws/runner/kcrest.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SubmitConnector PUTs the given config to /connectors/<name>/config. PUT is
// idempotent (creates or replaces), which is what we want for repeated sweep
// points within a single bench session.
func SubmitConnector(ctx context.Context, baseURL, name string, cfg map[string]any) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	url := fmt.Sprintf("%s/connectors/%s/config", baseURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT %s: status %d: %s", url, resp.StatusCode, string(raw))
	}
	return nil
}

type kcConnectorStatus struct {
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Trace string `json:"trace,omitempty"`
	} `json:"tasks"`
}

// WaitConnectorRunning polls GET /connectors/<name>/status until both the
// connector and every task report RUNNING, or until ctx is cancelled. If any
// task enters the FAILED state it returns immediately with the trace.
func WaitConnectorRunning(ctx context.Context, baseURL, name string, interval time.Duration) error {
	url := fmt.Sprintf("%s/connectors/%s/status", baseURL, name)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s RUNNING: %w", name, ctx.Err())
		default:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("GET %s: %w", url, err)
		}
		var status kcConnectorStatus
		if resp.StatusCode == http.StatusOK {
			_ = json.NewDecoder(resp.Body).Decode(&status)
		}
		resp.Body.Close()

		for _, t := range status.Tasks {
			if t.State == "FAILED" {
				return fmt.Errorf("connector %s task %d FAILED: %s", name, t.ID, t.Trace)
			}
		}
		if status.Connector.State == "RUNNING" && len(status.Tasks) > 0 {
			allRunning := true
			for _, t := range status.Tasks {
				if t.State != "RUNNING" {
					allRunning = false
					break
				}
			}
			if allRunning {
				return nil
			}
		}
		time.Sleep(interval)
	}
}

// DeleteConnector DELETEs /connectors/<name>. Returns nil for 200, 204, and
// 404 — the latter so callers can use it as an idempotent reset step without
// caring whether the connector already existed.
func DeleteConnector(ctx context.Context, baseURL, name string) error {
	url := fmt.Sprintf("%s/connectors/%s", baseURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", url, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE %s: status %d: %s", url, resp.StatusCode, string(raw))
	}
}
```

- [ ] **Step 4: Run tests — should pass**

```bash
go test ./benchmarking/aws/runner -run "TestSubmitConnector|TestWaitConnectorRunning|TestDeleteConnector" -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/kcrest.go benchmarking/aws/runner/kcrest_test.go
git commit -m "feat(bench/aws/kc): REST client (SubmitConnector, WaitConnectorRunning, DeleteConnector)"
```

---

### Task 10: `renderKCBenchScript` — per-sweep-point shell script for KC

**Files:**
- Create: `benchmarking/aws/runner/kcscript.go`
- Create: `benchmarking/aws/runner/kcscript_test.go`

The KC bench script is structurally different from Connect's:
- It restarts the KC worker JVM under `taskset + chrt -Xmx<N>g` (so the JVM gets the same CPU/memory budget as Connect for this sweep point).
- It waits for the REST API to be reachable.
- The connector submission + status-polling + delete happen via REST. We invoke a small Go helper binary (built once per session and staged on the runner) — OR — we shell out to `curl` directly. To keep Plan 2 simple and avoid shipping a second helper binary, **use `curl`**.
- The bench window is the same (warmup + duration).
- After SIGTERM of the JVM, captures stdout to `/tmp/kc-N.log` and uploads.

- [ ] **Step 1: Write failing tests**

Create `benchmarking/aws/runner/kcscript_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"
)

func TestRenderKCBenchScript_PinsCoresAndHeap(t *testing.T) {
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                4,
		MemLimitGiB:         4,
		WarmupSec:           60,
		DurationSec:         900,
		ConnectorName:       "bench_pg",
		ConnectorConfigJSON: `{"connector.class":"x"}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-xyz",
	})
	if !strings.Contains(script, "taskset -c 2-5") {
		t.Errorf("expected core mask 2-5 for vcpu=4; got:\n%s", script)
	}
	if !strings.Contains(script, "-Xmx4g") {
		t.Errorf("expected -Xmx4g heap; got:\n%s", script)
	}
	if !strings.Contains(script, "systemctl stop kafka-connect") {
		t.Errorf("expected stop of systemd-launched worker before our taskset-pinned spawn; got:\n%s", script)
	}
	if !strings.Contains(script, "connect-distributed.sh") {
		t.Errorf("expected connect-distributed.sh invocation; got:\n%s", script)
	}
	if !strings.Contains(script, "bench_pg") {
		t.Errorf("expected connector name in script; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_UploadsKCLog(t *testing.T) {
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                2,
		MemLimitGiB:         2,
		WarmupSec:           60,
		DurationSec:         600,
		ConnectorName:       "bench_my",
		ConnectorConfigJSON: `{}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-abc",
	})
	if !strings.Contains(script, `aws s3 cp "$KC_LOG" "s3://results-bucket/runs/sess-abc/kc-2.log"`) {
		t.Errorf("expected KC log upload; got:\n%s", script)
	}
}

func TestRenderKCBenchScript_SubmitsConnectorViaCurl(t *testing.T) {
	script := renderKCBenchScript(kcBenchScriptArgs{
		VCPU:                1,
		MemLimitGiB:         2,
		WarmupSec:           60,
		DurationSec:         600,
		ConnectorName:       "bench_pg",
		ConnectorConfigJSON: `{"connector.class":"io.debezium.connector.postgresql.PostgresConnector"}`,
		Bucket:              "results-bucket",
		SessionID:           "sess-abc",
	})
	if !strings.Contains(script, "curl") {
		t.Errorf("expected curl submission; got:\n%s", script)
	}
	if !strings.Contains(script, "/connectors/bench_pg/config") {
		t.Errorf("expected REST path; got:\n%s", script)
	}
	if !strings.Contains(script, "PUT") {
		t.Errorf("expected PUT method; got:\n%s", script)
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./benchmarking/aws/runner -run TestRenderKCBenchScript -v
```

Expected: `undefined: renderKCBenchScript` / `undefined: kcBenchScriptArgs`.

- [ ] **Step 3: Implement**

Create `benchmarking/aws/runner/kcscript.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strings"
)

type kcBenchScriptArgs struct {
	VCPU                int
	MemLimitGiB         int
	WarmupSec           int
	DurationSec         int
	ConnectorName       string
	ConnectorConfigJSON string // rendered JSON to PUT to /connectors/<name>/config
	Bucket              string
	SessionID           string
}

// renderKCBenchScript produces the shell script executed on the runner EC2
// for one KC sweep point. Unlike Connect's script:
//
//   1. The cloud-init-launched systemd kafka-connect.service is stopped
//      first, so we can spawn the JVM directly with taskset + chrt + Xmx
//      matching the current sweep point's vCPU/memory budget.
//   2. The connector is submitted via curl to the REST API once the JVM is
//      up. Submission is idempotent (PUT /connectors/<name>/config).
//   3. The connector's tasks do the actual CDC work; we sleep warmup+window
//      then SIGTERM the JVM, capture the log, upload, restart the systemd
//      unit so the next sweep point starts from a clean baseline.
func renderKCBenchScript(a kcBenchScriptArgs) string {
	cpusetHi := 1 + a.VCPU
	totalSec := a.WarmupSec + a.DurationSec
	// Escape single quotes inside the JSON body for the heredoc.
	cfgJSON := strings.ReplaceAll(a.ConnectorConfigJSON, "'", `'"'"'`)

	lines := []string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting kc bench: %d vCPU, %d GiB heap, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`KC_LOG=/tmp/kc-%d.log`, a.VCPU),
		`: > "$KC_LOG"`,
		// Stop the cloud-init-launched worker so we can spawn under taskset.
		`sudo systemctl stop kafka-connect || true`,
		`sleep 2`,
		// Spawn the JVM directly. Equivalent to the systemd unit's ExecStart
		// but with vCPU + heap pinned for this sweep point.
		fmt.Sprintf(`taskset -c 2-%d chrt --fifo 50 env KAFKA_HEAP_OPTS=-Xmx%dg /opt/kafka/bin/connect-distributed.sh /opt/kafka-connect/worker.properties >"$KC_LOG" 2>&1 &`,
			cpusetHi, a.MemLimitGiB),
		`PID=$!`,
		// Wait until the REST API answers.
		`for i in $(seq 1 60); do
  if curl -fsS http://localhost:8083/ >/dev/null 2>&1; then break; fi
  sleep 1
done`,
		// Submit the connector. Body comes from the heredoc below.
		fmt.Sprintf(`cat > /tmp/kc-cfg-%d.json <<'KCCFG'
%s
KCCFG`, a.VCPU, cfgJSON),
		fmt.Sprintf(`curl -fsS -X PUT -H 'Content-Type: application/json' --data-binary @/tmp/kc-cfg-%d.json http://localhost:8083/connectors/%s/config`,
			a.VCPU, a.ConnectorName),
		// Wait for RUNNING status (up to 60s).
		fmt.Sprintf(`for i in $(seq 1 60); do
  STATE=$(curl -fsS http://localhost:8083/connectors/%s/status | jq -r '.tasks[0].state // "unknown"' 2>/dev/null || echo "unknown")
  if [ "$STATE" = "RUNNING" ]; then break; fi
  if [ "$STATE" = "FAILED" ]; then echo "task FAILED before warmup; aborting"; exit 1; fi
  sleep 1
done`, a.ConnectorName),
		// Heartbeat — every 60s, print last lines of KC_LOG so SSM has signal.
		`(
  while kill -0 "$PID" 2>/dev/null; do
    sleep 60
    LATEST="$(tail -n 1 "$KC_LOG" 2>/dev/null | tr -d '\n' || true)"
    echo "[kc-heartbeat] ${LATEST:-no output yet}"
  done
) &`,
		`HEARTBEAT=$!`,
		fmt.Sprintf(`sleep %d`, totalSec),
		// Tear down the connector + the JVM.
		fmt.Sprintf(`curl -fsS -X DELETE http://localhost:8083/connectors/%s || true`, a.ConnectorName),
		`kill -TERM "$PID" 2>/dev/null || true`,
		`wait "$PID" 2>/dev/null || true`,
		`kill "$HEARTBEAT" 2>/dev/null || true`,
		`echo "kc bench point complete"`,
		fmt.Sprintf(`aws s3 cp "$KC_LOG" "s3://%s/runs/%s/kc-%d.log" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
		`echo "kc log uploaded"`,
		// Restart the systemd unit so the host is ready for the next point.
		`sudo systemctl start kafka-connect || true`,
	}
	return strings.Join(lines, "\n")
}
```

- [ ] **Step 4: Run tests — should pass**

```bash
go test ./benchmarking/aws/runner -run TestRenderKCBenchScript -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/kcscript.go benchmarking/aws/runner/kcscript_test.go
git commit -m "feat(bench/aws/kc): renderKCBenchScript (per-sweep-point JVM lifecycle + REST)"
```

---

### Task 11: Matrix runner — engine-inner loop

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go`
- Modify: `benchmarking/aws/runner/matrix_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `benchmarking/aws/runner/matrix_test.go`:

```go
func TestMatrixRun_EngineInnerLoop_BothEngines(t *testing.T) {
	fakeSSM := &FakeSSM{}
	mr := &MatrixRunner{
		SSM:             fakeSSM,
		LogFetcher:      &FakeLogFetcher{},
		RunnerInstance:  "i-runner",
		LoadGenInstance: "i-loadgen",
		ConfigPath:      "/opt/bench/config.yaml",
		BinaryPath:      "/opt/bench/redpanda-connect",
		Bucket:          "bkt",
		SessionID:       "sess",
		// Engines is the field we're adding.
		Engines: []string{"connect", "kafka_connect"},
		// KC-specific fields:
		KCConnectorName:       "bench_pg",
		KCConnectorConfigJSON: `{"connector.class":"x"}`,
	}
	points, err := mr.Run(
		context.Background(),
		[]int{1, 2},
		2,
		0,
		1*time.Second,
		"reset-script",
		"workload-script",
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 2 cpu points × 2 engines = 4 sweep points.
	if len(points) != 4 {
		t.Fatalf("expected 4 sweep points (2 vcpu × 2 engines); got %d", len(points))
	}
	// Order: vcpu outer, engine inner.
	wantOrder := []struct{ vcpu int; engine string }{
		{1, "connect"},
		{1, "kafka_connect"},
		{2, "connect"},
		{2, "kafka_connect"},
	}
	for i, w := range wantOrder {
		if points[i].VCPU != w.vcpu {
			t.Errorf("points[%d].VCPU = %d, want %d", i, points[i].VCPU, w.vcpu)
		}
		if points[i].Engine != w.engine {
			t.Errorf("points[%d].Engine = %s, want %s", i, points[i].Engine, w.engine)
		}
	}
}

func TestMatrixRun_EngineInnerLoop_ConnectOnly(t *testing.T) {
	fakeSSM := &FakeSSM{}
	mr := &MatrixRunner{
		SSM:             fakeSSM,
		LogFetcher:      &FakeLogFetcher{},
		RunnerInstance:  "i-runner",
		LoadGenInstance: "i-loadgen",
		ConfigPath:      "/opt/bench/config.yaml",
		BinaryPath:      "/opt/bench/redpanda-connect",
		Bucket:          "bkt",
		SessionID:       "sess",
		Engines:         []string{"connect"},
	}
	points, err := mr.Run(
		context.Background(),
		[]int{1, 2, 4},
		2,
		0,
		1*time.Second,
		"reset-script",
		"workload-script",
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(points) != 3 {
		t.Fatalf("expected 3 sweep points (connect-only); got %d", len(points))
	}
	for _, p := range points {
		if p.Engine != "connect" {
			t.Errorf("expected engine=connect; got %s", p.Engine)
		}
	}
}
```

(`FakeSSM` and `FakeLogFetcher` already exist in the test files — confirm before writing the tests; if their constructors look different, adapt.)

- [ ] **Step 2: Run — expect failure**

```bash
go test ./benchmarking/aws/runner -run TestMatrixRun_EngineInnerLoop -v
```

Expected: build error or test failure (the `Engines` and `KC*` fields don't exist on `MatrixRunner`).

- [ ] **Step 3: Add fields to `MatrixRunner`**

In `benchmarking/aws/runner/matrix.go`, extend `MatrixRunner`:

```go
type MatrixRunner struct {
	SSM             SSMExecutor
	LogFetcher      LogFetcher
	RunnerInstance  string
	LoadGenInstance string
	ConfigPath      string
	BinaryPath      string
	Bucket          string
	SessionID       string
	// RedpandaMetricsEndpoint is the host:port pair (e.g. "10.42.10.10:9644") the
	// per-point scraper curls every 10s. Empty disables the scraper.
	RedpandaMetricsEndpoint string
	// Engines lists the engines to sweep at each vCPU point, in order.
	// Default ["connect"] preserves the pre-Plan-2 behavior.
	Engines []string
	// KCConnectorName is the name to submit the KC connector under.
	// Empty when Engines does not include "kafka_connect".
	KCConnectorName string
	// KCConnectorConfigJSON is the rendered JSON config posted to KC's REST API.
	KCConnectorConfigJSON string
}
```

- [ ] **Step 4: Refactor `Run` to add the engine-inner loop**

Replace the body of `Run` (currently iterates only over `cpuPoints`) with:

```go
func (m *MatrixRunner) Run(
	ctx context.Context,
	cpuPoints []int,
	memLimitPerVCPU int,
	warmup, duration time.Duration,
	resetScript string,
	workloadScript string,
) ([]SweepPoint, error) {
	engines := m.Engines
	if len(engines) == 0 {
		engines = []string{"connect"}
	}
	out := make([]SweepPoint, 0, len(cpuPoints)*len(engines))
	for _, n := range cpuPoints {
		for _, engine := range engines {
			fmt.Printf("=== sweep point: %d vCPU, engine=%s (warmup %s, window %s) ===\n", n, engine, warmup, duration)

			if resetScript != "" {
				if err := m.SSM.Run(ctx, m.RunnerInstance, resetScript, streamingOnLine(stdout, "reset")); err != nil {
					return nil, fmt.Errorf("reset at %d vCPU (%s): %w", n, engine, err)
				}
			}

			workloadCtx, cancelWorkload := context.WithCancel(ctx)
			workloadDone := make(chan error, 1)
			if workloadScript != "" {
				go func() {
					workloadDone <- m.SSM.Run(workloadCtx, m.LoadGenInstance, workloadScript, streamingOnLine(stdout, "load"))
				}()
			} else {
				close(workloadDone)
			}

			// Build the per-engine bench script.
			var script string
			switch engine {
			case "connect":
				script = renderBenchScript(benchScriptArgs{
					VCPU:                    n,
					MemLimitGiB:             memLimitPerVCPU * n,
					WarmupSec:               int(warmup.Seconds()),
					DurationSec:             int(duration.Seconds()),
					ConfigPath:              m.ConfigPath,
					BinaryPath:              m.BinaryPath,
					Bucket:                  m.Bucket,
					SessionID:               m.SessionID,
					RedpandaMetricsEndpoint: m.RedpandaMetricsEndpoint,
				})
			case "kafka_connect":
				script = renderKCBenchScript(kcBenchScriptArgs{
					VCPU:                n,
					MemLimitGiB:         memLimitPerVCPU * n,
					WarmupSec:           int(warmup.Seconds()),
					DurationSec:         int(duration.Seconds()),
					ConnectorName:       m.KCConnectorName,
					ConnectorConfigJSON: m.KCConnectorConfigJSON,
					Bucket:              m.Bucket,
					SessionID:           m.SessionID,
				})
			default:
				cancelWorkload()
				<-workloadDone
				return nil, fmt.Errorf("unknown engine %q at vcpu %d", engine, n)
			}

			if err := m.SSM.Run(ctx, m.RunnerInstance, script, streamingOnLine(stdout, fmt.Sprintf("bench-%s", engine))); err != nil {
				cancelWorkload()
				<-workloadDone
				return nil, fmt.Errorf("bench at %d vCPU (%s): %w", n, engine, err)
			}

			cancelWorkload()
			<-workloadDone

			// Per-engine log fetch + parse — these helpers must accept engine.
			samples := m.fetchAndParse(ctx, engine, n, warmup)
			prom := m.fetchProm(ctx, n)
			point := SweepPoint{
				VCPU:    n,
				Engine:  engine,
				Samples: samples,
				Summary: Summarise(samples),
				Prom:    prom,
			}
			out = append(out, point)
		}
	}
	return out, nil
}
```

- [ ] **Step 5: Extend `fetchAndParse` to be engine-aware**

The current `fetchLog` returns the Connect log. For KC, we need to fetch `runs/<sess>/kc-N.log`. The simplest approach: introduce a small helper.

Find the existing log-fetch helper in `matrix.go` (look for the `parseAndTrim` / `fetchLog` code, around lines 110-150). Update to accept an `engine` parameter and pick the right S3 key:

```go
func (m *MatrixRunner) fetchAndParse(ctx context.Context, engine string, vcpu int, warmup time.Duration) []Sample {
	if m.LogFetcher == nil {
		return nil
	}
	var key string
	switch engine {
	case "connect":
		key = fmt.Sprintf("runs/%s/sweep-%d.log", m.SessionID, vcpu)
	case "kafka_connect":
		key = fmt.Sprintf("runs/%s/kc-%d.log", m.SessionID, vcpu)
	default:
		return nil
	}
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, key)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] fetch %s log (non-fatal): %v\n", engine, err)
		return nil
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil
	}
	// Connect emits the rolling-stats line format that ParseRollingStatsStream understands.
	// KC emits its own log shape — for Plan 2 we parse Connect logs; KC's throughput
	// comes via Plan 3's broker-side parser. Until then, KC samples are empty.
	if engine == "connect" {
		return parseAndTrim(raw, warmup)
	}
	return nil
}
```

Then delete/replace the previous `fetchLog` call site in `Run` if it existed; the new `fetchAndParse` replaces it.

- [ ] **Step 6: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestMatrixRun -v
```

Expected: both new tests PASS. The existing matrix tests should still pass — they construct `MatrixRunner` without setting `Engines`, which defaults to `["connect"]`.

- [ ] **Step 7: Full suite**

```bash
go test ./benchmarking/aws/runner/...
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go
git commit -m "feat(bench/aws): engine-inner sweep loop, per-engine bench script dispatch"
```

---

### Task 12: Reset extensions — topic delete + KC connector DELETE

**Files:**
- Modify: `benchmarking/aws/runner/scripts.go`
- Modify: `benchmarking/aws/runner/scripts_test.go`

- [ ] **Step 1: Write failing tests**

Append to `benchmarking/aws/runner/scripts_test.go`:

```go
func TestCombineReset_AppendsKCAndTopicCleanup_Postgres(t *testing.T) {
	outs := map[string]string{
		"postgres_dsn":              "postgres://user:pw@host/db",
		"bench_session_id":          "sess-abc",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}
	steps := []ResetStep{
		{SQL: "SELECT 1"},
	}
	out, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	// Existing SQL still present.
	if !strings.Contains(out, "SELECT 1") {
		t.Errorf("expected original SQL to remain; got:\n%s", out)
	}
	// KC connector idempotent delete.
	if !strings.Contains(out, "curl") || !strings.Contains(out, "X DELETE") {
		t.Errorf("expected idempotent KC connector DELETE; got:\n%s", out)
	}
	// Topic deletes for both engines via kafka-topics.sh.
	if !strings.Contains(out, "kafka-topics.sh") {
		t.Errorf("expected kafka-topics.sh delete; got:\n%s", out)
	}
	if !strings.Contains(out, "bench_sess-abc_postgres_cdc_connect") {
		t.Errorf("expected Connect topic delete; got:\n%s", out)
	}
	if !strings.Contains(out, "bench_sess-abc_postgres_cdc_kc") {
		t.Errorf("expected KC topic delete; got:\n%s", out)
	}
}

func TestCombineReset_NoOpWhenSessionIDMissing(t *testing.T) {
	// If bench_session_id is somehow unset, the reset should skip the
	// topic/connector cleanup steps rather than emit malformed commands.
	outs := map[string]string{
		"postgres_dsn":              "postgres://user:pw@host/db",
		"redpanda_broker_endpoints": "10.42.10.10:9092",
	}
	steps := []ResetStep{{SQL: "SELECT 1"}}
	out, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if strings.Contains(out, "kafka-topics.sh") {
		t.Errorf("topic delete should be skipped when session id is empty; got:\n%s", out)
	}
}
```

- [ ] **Step 2: Run — expect failure (reset doesn't add the new commands yet)**

```bash
go test ./benchmarking/aws/runner -run TestCombineReset_AppendsKC -v
```

Expected: FAIL.

- [ ] **Step 3: Extend `combineReset` in `scripts.go`**

The existing function uses a `strings.Builder` (`var sb strings.Builder` → `return sb.String(), nil`). Before the `return sb.String(), nil` at the end of `combineReset`, add the KC + topic cleanup commands gated on session ID + brokers being present:

```go
	// After the existing for-loop over `steps`, before the return:

	sessionID := outs["bench_session_id"]
	brokers := outs["redpanda_broker_endpoints"]
	if sessionID != "" && brokers != "" {
		// Idempotent KC connector delete (404 is fine).
		sb.WriteString(fmt.Sprintf(
			`curl -fsS -X DELETE "http://localhost:8083/connectors/bench_%s" || true`+"\n",
			connector,
		))
		// Delete both engines' topics. Errors are fine — topic may not exist.
		for _, engine := range []string{"connect", "kc"} {
			topic := fmt.Sprintf("bench_%s_%s_%s", sessionID, connector, engine)
			sb.WriteString(fmt.Sprintf(
				`/opt/kafka/bin/kafka-topics.sh --bootstrap-server %q --delete --topic %q 2>/dev/null || true`+"\n",
				brokers, topic,
			))
		}
	}

	return sb.String(), nil
}
```

Note: `combineReset` early-returns `("", nil)` when `len(steps) == 0`. That preserves existing behavior — scenarios without reset SQL still get an empty reset script. The new cleanup only fires when reset steps exist AND the session/broker outputs are populated.

- [ ] **Step 4: Run tests**

```bash
go test ./benchmarking/aws/runner -run TestCombineReset -v
```

Expected: PASS.

- [ ] **Step 5: Full suite**

```bash
go test ./benchmarking/aws/runner/...
```

Expected: PASS. Existing reset tests should be unaffected because the new appends are gated on `sessionID != "" && brokers != ""` — old tests that don't set those values keep their previous behavior.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/scripts.go benchmarking/aws/runner/scripts_test.go
git commit -m "feat(bench/aws): reset extension — delete topics + KC connector between sweep points"
```

---

### Task 13: Wire `--engines` flag + KC config rendering into `runBench`

**Files:**
- Modify: `benchmarking/aws/runner/main.go`

- [ ] **Step 1: Add `--engines` to the CLI flag set**

Find the CLI flag parsing in `main.go` (look for `flag.NewFlagSet("bench"` or similar). Add an `--engines` flag:

```go
var enginesStr string
fs.StringVar(&enginesStr, "engines", "connect,kafka_connect",
	"Comma-separated list of engines to sweep at each vCPU point. Default runs both Connect and Kafka Connect side-by-side.")
```

Parse it after `fs.Parse(args)`:

```go
engines := strings.Split(enginesStr, ",")
for i, e := range engines {
	engines[i] = strings.TrimSpace(e)
}
```

- [ ] **Step 2: Pre-render KC connector config + parse DSN**

In `runBench`, after `LoadScenario(...)` and after the TF outputs map is built, render the KC config (only if `kafka_connect` is in `engines`):

```go
var kcConnectorName, kcConfigJSON string
if slices.Contains(engines, "kafka_connect") {
	es, ok := engineSpecFor(s.Connector)
	if !ok {
		return fmt.Errorf("no engineSpec for %q", s.Connector)
	}
	// Parse DSN from outs into host/port/user/pass/db parts. For Postgres
	// the engineSpec doesn't carry discrete reset fields, so we parse the
	// DSN URL inline. For MySQL the discrete reset outputs already exist.
	in, err := buildKCRenderInputs(s, es, sharedOuts, sessionID)
	if err != nil {
		return fmt.Errorf("build KC render inputs: %w", err)
	}
	cfg, err := renderKCConfig(s, in)
	if err != nil {
		return fmt.Errorf("render KC config: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	kcConnectorName = fmt.Sprintf("bench_%s", s.Connector)
	kcConfigJSON = string(raw)
}
```

- [ ] **Step 3: Implement `buildKCRenderInputs` helper**

In `main.go` (or a new file `kcinputs.go` if `main.go` is getting heavy), add:

```go
func buildKCRenderInputs(s *Scenario, es engineSpec, outs map[string]string, sessionID string) (kcRenderInputs, error) {
	in := kcRenderInputs{
		TopicPrefix:      fmt.Sprintf("bench_%s_%s_kc", sessionID, s.Connector),
		BootstrapServers: outs["redpanda_broker_endpoints"],
	}
	// Tables come from the scenario's pipeline.input map.
	if inputMap, ok := s.Pipeline["input"].(map[string]any); ok {
		for _, v := range inputMap {
			if connMap, ok := v.(map[string]any); ok {
				if tbls, ok := connMap["tables"].([]any); ok {
					for _, t := range tbls {
						if ts, ok := t.(string); ok {
							in.Tables = append(in.Tables, ts)
						}
					}
				}
			}
		}
	}

	// SchemaTables formatting depends on engine.
	switch s.Connector {
	case "postgres_cdc":
		schema := "public"
		if inputMap, ok := s.Pipeline["input"].(map[string]any); ok {
			if pgMap, ok := inputMap["postgres_cdc"].(map[string]any); ok {
				if sc, ok := pgMap["schema"].(string); ok {
					schema = sc
				}
			}
		}
		var sb strings.Builder
		for i, t := range in.Tables {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(schema + "." + t)
		}
		in.SchemaTables = sb.String()
	case "mysql_cdc":
		var sb strings.Builder
		for i, t := range in.Tables {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(in.Database + "." + t)
		}
		in.SchemaTables = sb.String()
	}

	// Connection parts.
	if es.ResetHostOutputKey != "" {
		// MySQL-style: discrete TF outputs.
		in.Host = outs[es.ResetHostOutputKey]
		in.Port = outs[es.ResetPortOutputKey]
		in.User = outs[es.ResetUserOutputKey]
		in.Password = outs[es.ResetPassOutputKey]
		in.Database = outs[es.ResetDBOutputKey]
	} else {
		// Postgres-style: parse DSN URL.
		dsn := outs[es.DSNOutputKey]
		u, err := url.Parse(dsn)
		if err != nil {
			return in, fmt.Errorf("parse DSN %q: %w", dsn, err)
		}
		in.Host = u.Hostname()
		in.Port = u.Port()
		if in.Port == "" {
			in.Port = "5432"
		}
		if u.User != nil {
			in.User = u.User.Username()
			pw, _ := u.User.Password()
			in.Password = pw
		}
		in.Database = strings.TrimPrefix(u.Path, "/")
	}
	return in, nil
}
```

Add `"net/url"`, `"encoding/json"`, `"slices"` to imports at the top of `main.go` if not present.

- [ ] **Step 4: Pass engines + KC fields into `MatrixRunner`**

Locate the `MatrixRunner` construction in `runBench` (around line 220 from Plan 1) and extend it:

```go
mr := &MatrixRunner{
	SSM:                     ssmExec,
	LogFetcher:              logFetcher,
	RunnerInstance:          sharedOuts["runner_instance_id"],
	LoadGenInstance:         sharedOuts["load_gen_instance_id"],
	ConfigPath:              "/opt/bench/config.yaml",
	BinaryPath:              "/opt/bench/redpanda-connect",
	Bucket:                  sharedOuts["results_bucket"],
	SessionID:               sessionID,
	RedpandaMetricsEndpoint: sharedOuts["redpanda_metrics_endpoint"],
	Engines:                 engines,
	KCConnectorName:         kcConnectorName,
	KCConnectorConfigJSON:   kcConfigJSON,
}
```

- [ ] **Step 5: Build + run all tests**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
go build ./benchmarking/aws/runner/...
go test ./benchmarking/aws/runner/...
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): wire --engines flag and KC config rendering into runBench"
```

---

### Task 14: README Plan 2 section + Taskfile update

**Files:**
- Modify: `benchmarking/aws/README.md`

- [ ] **Step 1: Append a section to the README**

Add to `benchmarking/aws/README.md` at the bottom:

```markdown
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

**Known limitation:** Plan 2 captures KC's per-sweep-point log to S3 but doesn't parse it. The canonical KC throughput number comes from the broker-side metric introduced in Plan 1 (uploaded to S3 as `redpanda-<vcpu>.txt`), and that's what Plan 3 will surface in reporting. Until Plan 3 lands, the KC `PointResult.Summary` is the zero value (`MedianMBPerSec: 0`) — the data is in S3 but not yet rendered into the markdown.
```

- [ ] **Step 2: Commit**

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws): document Plan 2 (engine-inner loop, KC submit-by-REST)"
```

---

### Task 15: AWS smoke test (operator-driven)

**Files:** (no code changes; manual verification)

This is the operator-driven acceptance gate for Plan 2. It requires Plan 1's Task 12 to have been done first (Redpanda + KC infra applied to a real account).

- [ ] **Step 1: Build the runner binary**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
aws-vault exec rp-bench -- task -d benchmarking/aws aws:validate
```

Expected: `validate: OK`.

- [ ] **Step 2: Run a small smoke bench with both engines**

Use `/tmp/plan1-smoke.yaml` from Plan 1's Task 12 (if it still exists), or recreate it. Make sure the scenario YAML has `cpu_points: [1]` (single point keeps wall-clock short).

```bash
aws-vault exec rp-bench -- task -d benchmarking/aws aws:bench SCENARIO=/tmp/plan1-smoke.yaml
```

Expected wall-clock: ~6-8 minutes (vs ~3-4 min in Plan 1). Two engine passes at vCPU 1 = 2 × (warmup + 2m duration + reset).

- [ ] **Step 3: Verify result JSON has two engines**

Find the latest result JSON:

```bash
ls -t docs/benchmark-results/results/postgres_cdc/postgres-orders-cdc/ | head -1
jq '[.points[] | {vcpu, engine, median_mb: .summary.median_mb_per_sec}]' docs/benchmark-results/results/postgres_cdc/postgres-orders-cdc/<latest>.json
```

Expected: 2 entries, both at `vcpu: 1`, one `engine: "connect"` with non-zero median, one `engine: "kafka_connect"` with zero median (Plan 2 doesn't parse KC throughput yet).

- [ ] **Step 4: Verify the KC log was uploaded to S3**

```bash
aws-vault exec rp-bench -- aws s3 ls s3://<results-bucket>/runs/<session-id>/
```

Expected: listing includes `kc-1.log` (KC's log) alongside `sweep-1.log` (Connect), `prom-1.txt`, `redpanda-1.txt`.

- [ ] **Step 5: Verify the KC topic exists in Redpanda**

```bash
aws-vault exec rp-bench -- aws ssm start-session --target $(terraform -chdir=benchmarking/aws/terraform/shared output -raw runner_instance_id)
$ /opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.42.10.10:9092 --list | grep bench_
```

Expected: at least two `bench_<session>_postgres_cdc_*` topics (or none if reset deleted them at the end, which is also fine — the in-flight verification is more important).

While in the SSM session, can also re-verify the running connector path:

```bash
$ curl -s localhost:8083/connectors
```

(After bench teardown this should be empty `[]` since the bench DELETEd it.)

- [ ] **Step 6: Tear down**

```bash
aws-vault exec rp-bench -- task -d benchmarking/aws aws:down
```

If steps 2-5 pass, **commit a brief note to the README's Plan 2 section** with the session ID and a one-line summary.

If steps 2-5 FAIL: diagnose. Likely failure modes:
- **KC worker doesn't restart cleanly**: check `/var/log/kafka-connect.service.log` or `journalctl -u kafka-connect`. May need to add `sleep N` after `systemctl stop` in `renderKCBenchScript` Step 3.
- **Connector submission fails**: check `curl localhost:8083/connector-plugins` to confirm the class is actually loaded. If `io.aiven.connect.jdbc.JdbcSinkConnector` vs `io.aiven.kafka.connect.jdbc.JdbcSinkConnector` mismatch, that's the Plan 1 follow-up; not a Plan 2 issue.
- **`schema.history` topic creation fails (MySQL only)**: Redpanda's topic auto-create may need to be enabled in `redpanda.yaml`. Add `auto_create_topics_enabled: true` to the cluster config and re-apply.

---

## Verification checklist (Plan 2 acceptance)

- [ ] `go build ./benchmarking/aws/runner/...` clean
- [ ] `go test ./benchmarking/aws/runner/...` green
- [ ] `terraform validate` still passes for both `terraform/shared` and `terraform/modules/redpanda` (Plan 2 should not have touched TF)
- [ ] Smoke bench (Task 15) produces a result JSON with two `PointResult` rows per vCPU (one connect, one kafka_connect)
- [ ] `s3://<results>/runs/<sess>/kc-1.log` exists and is non-empty after smoke run
- [ ] `--engines connect` runs single-engine and produces results matching pre-Plan-2 byte-shape
