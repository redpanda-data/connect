# AWS benchmarking — mysql_cdc Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Onboard `mysql_cdc` as the second connector in the AWS bench framework — new TF module, new stack, new seeder, new scenario YAML — plus a small engine abstraction in the runner so future connectors plug in via a one-entry map.

**Architecture:** Connector-keyed `engineSpec` map (`postgres_cdc`, `mysql_cdc`) replaces three hardcoded Postgres references in the runner (seed/reset/workload script rendering). New `rds-mysql` Terraform module and `mysql` stack mirror the postgres equivalents with binlog params + `backup_retention_period=1`. New `cdc-rows-mysql` seeder uses `database/sql` + `go-sql-driver/mysql`. Scenario YAML uses `initial_rows: 0`, `TRUNCATE TABLE orders` between sweep points, memory `checkpoint_cache`.

**Tech Stack:** Go, Terraform, AWS RDS MySQL 8.0.46, `go-sql-driver/mysql`, AL2023 arm64, MariaDB client (for reset SQL).

**Spec:** [`docs/superpowers/specs/2026-05-21-aws-benchmarking-mysql-design.md`](../specs/2026-05-21-aws-benchmarking-mysql-design.md)

---

## File Structure

Existing files modified:

- `benchmarking/aws/runner/scenario.go` — add `engineSpec` type + `engineSpecs` map + `engineSpecFor()` helper; tighten `Validate()` to reject unknown connectors.
- `benchmarking/aws/runner/main.go` — gut the three hardcoded sites (renderSeedScript, combineReset, renderWorkloadScript) and call new helpers in `scripts.go`.
- `benchmarking/aws/terraform/shared/runner.tf` — add `mariadb1011` to user-data packages.
- `benchmarking/aws/README.md` — append a MySQL section.

New files created:

- `benchmarking/aws/runner/scripts.go` — extracted `renderSeedScript`/`combineReset`/`renderWorkloadScript` (engine-aware).
- `benchmarking/aws/runner/scripts_test.go` — unit tests for the three renderers (postgres + mysql paths).
- `benchmarking/aws/runner/scenario_test.go` — extend with engine-spec map tests (in same file as existing scenario tests).
- `benchmarking/aws/terraform/modules/rds-mysql/{main.tf,variables.tf,outputs.tf}` — new TF module.
- `benchmarking/aws/terraform/stacks/mysql/{main.tf,variables.tf,outputs.tf}` — new TF stack.
- `benchmarking/aws/seeders/cdc-rows-mysql/{main.go,sql.go}` — new seeder.
- `benchmarking/aws/scenarios/mysql/orders-cdc.yaml` — new scenario.
- `docs/benchmark-results/mysql.md` — header skeleton; AWS rows append below.

Why this split: the three render helpers are tightly related and currently inline in `main.go` (lines ~580–628). Pulling them into `scripts.go` lets them be unit-tested and isolates the engine-abstraction logic from the orchestration code in main.go. The engine map lives in `scenario.go` next to `Scenario.Validate()` because validation depends on it.

---

## Task ordering

Code-only tasks (1–6) come first so the unit tests cover the runner generalisation before we layer on infra. TF + scenario + smoke (7–13) follow.

---

### Task 1: Add `engineSpec` map to scenario.go

**Files:**
- Modify: `benchmarking/aws/runner/scenario.go`
- Test: `benchmarking/aws/runner/scenario_test.go`

- [ ] **Step 1: Write failing tests**

Append to `benchmarking/aws/runner/scenario_test.go`:

```go
func TestEngineSpecFor_Postgres(t *testing.T) {
	es, ok := engineSpecFor("postgres_cdc")
	if !ok {
		t.Fatalf("postgres_cdc should be registered")
	}
	if es.DSNOutputKey != "postgres_dsn" {
		t.Errorf("DSNOutputKey = %q, want postgres_dsn", es.DSNOutputKey)
	}
	if es.DSNEnvVar != "POSTGRES_DSN" {
		t.Errorf("DSNEnvVar = %q, want POSTGRES_DSN", es.DSNEnvVar)
	}
	if es.ResetHostOutputKey != "" {
		t.Errorf("postgres should use DSN-style reset, not host/port; got ResetHostOutputKey=%q", es.ResetHostOutputKey)
	}
}

func TestEngineSpecFor_MySQL(t *testing.T) {
	es, ok := engineSpecFor("mysql_cdc")
	if !ok {
		t.Fatalf("mysql_cdc should be registered")
	}
	if es.DSNOutputKey != "mysql_dsn" {
		t.Errorf("DSNOutputKey = %q, want mysql_dsn", es.DSNOutputKey)
	}
	if es.DSNEnvVar != "MYSQL_DSN" {
		t.Errorf("DSNEnvVar = %q, want MYSQL_DSN", es.DSNEnvVar)
	}
	if es.ResetHostOutputKey != "mysql_host" {
		t.Errorf("ResetHostOutputKey = %q, want mysql_host", es.ResetHostOutputKey)
	}
	if es.ResetPortOutputKey != "mysql_port" || es.ResetUserOutputKey != "mysql_user" ||
		es.ResetPassOutputKey != "mysql_password" || es.ResetDBOutputKey != "mysql_db" {
		t.Errorf("mysql reset output keys incomplete: %+v", es)
	}
}

func TestEngineSpecFor_Unknown(t *testing.T) {
	if _, ok := engineSpecFor("kafka_franz_in_disguise"); ok {
		t.Error("unknown connector should not resolve")
	}
}

func TestValidate_RejectsUnknownConnector(t *testing.T) {
	s := &Scenario{
		Name: "bad", Connector: "kafka_franz_in_disguise", Stack: "kafka",
		Infra: InfraSpec{Runner: RunnerSpec{InstanceType: "c8g.4xlarge"}},
		Matrix: MatrixSpec{CPUPoints: []int{1, 2}},
		Workload: &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute, WriteRatePerSec: 1000},
	}
	err := s.Validate()
	if err == nil {
		t.Fatal("expected unknown-connector error")
	}
	if !strings.Contains(err.Error(), "kafka_franz_in_disguise") {
		t.Errorf("error should name the unknown connector; got: %v", err)
	}
}
```

Add the necessary imports to `scenario_test.go` if not already present (`strings`, `time`).

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/ -run 'TestEngineSpec|TestValidate_RejectsUnknownConnector' -v
```

Expected: FAIL with `undefined: engineSpecFor`.

- [ ] **Step 3: Implement engineSpec + map + lookup**

Edit `benchmarking/aws/runner/scenario.go`. After the existing type declarations (after `ResetStep`, before `LoadScenario`), add:

```go
// engineSpec captures the per-engine wiring needed to render seed/reset/workload
// scripts. Adding a new engine means adding a new entry in engineSpecs; no
// switch-statement edits anywhere else.
type engineSpec struct {
	// DSNOutputKey is the terraform output key holding the connection string.
	DSNOutputKey string
	// DSNEnvVar is the env var name to set in seed/workload scripts.
	DSNEnvVar string
	// For reset commands, the CLI tool may or may not accept a DSN URL. When
	// the engine's CLI does (e.g. psql), we leave the Reset*OutputKey fields
	// empty and the reset builder uses the DSN form. When it does not (e.g.
	// mysql, which wants discrete -h/-P/-u/-p flags), the Reset*OutputKey
	// fields point at terraform outputs and the reset builder uses those.
	ResetHostOutputKey string
	ResetPortOutputKey string
	ResetUserOutputKey string
	ResetPassOutputKey string
	ResetDBOutputKey   string
}

var engineSpecs = map[string]engineSpec{
	"postgres_cdc": {
		DSNOutputKey: "postgres_dsn",
		DSNEnvVar:    "POSTGRES_DSN",
	},
	"mysql_cdc": {
		DSNOutputKey:       "mysql_dsn",
		DSNEnvVar:          "MYSQL_DSN",
		ResetHostOutputKey: "mysql_host",
		ResetPortOutputKey: "mysql_port",
		ResetUserOutputKey: "mysql_user",
		ResetPassOutputKey: "mysql_password",
		ResetDBOutputKey:   "mysql_db",
	},
}

func engineSpecFor(connector string) (engineSpec, bool) {
	es, ok := engineSpecs[connector]
	return es, ok
}
```

Then in `Scenario.Validate()`, immediately after the `if s.Connector == ""` block, add:

```go
	if _, ok := engineSpecFor(s.Connector); !ok {
		return fmt.Errorf("connector %q has no engineSpec entry; add one to engineSpecs in scenario.go", s.Connector)
	}
```

- [ ] **Step 4: Run tests to verify pass**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/ -run 'TestEngineSpec|TestValidate_RejectsUnknownConnector' -v
```

Expected: PASS (4 tests).

- [ ] **Step 5: Run the full scenario test file to confirm no regressions**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/ -run 'TestValidate|TestLoad|TestScenario|TestEngineSpec' -v
```

Expected: PASS. All existing postgres scenarios still validate.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/scenario.go benchmarking/aws/runner/scenario_test.go
git commit -m "feat(bench/aws): engineSpec map for connector→engine wiring

Add engineSpec + engineSpecs map + engineSpecFor lookup. Validate now
rejects scenarios whose connector has no engineSpec entry. Postgres
maps as before (DSN-style reset); mysql_cdc lands with discrete
host/port/user/pass/db output keys for the mysql CLI.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Extract script renderers into scripts.go (engine-aware)

**Files:**
- Create: `benchmarking/aws/runner/scripts.go`
- Create: `benchmarking/aws/runner/scripts_test.go`
- Modify: `benchmarking/aws/runner/main.go` — delete the three render funcs (lines ~580–628) and the `runSeeder` script literal; replace with calls into `scripts.go`.

The render funcs we extract are:
1. `renderSeedScript(s, outs)` — was the literal in `runSeeder` at main.go:580
2. `combineReset(steps, outs)` — was main.go:592
3. `renderWorkloadScript(s, outs)` — was main.go:611

`combineReset` already exists at main.go:592 as a function with the right signature — we move it into scripts.go untouched (except for engine-aware logic) and delete the original.

`renderSeedScript` does not exist as a function — the body is inline in `runSeeder` at main.go:584-590. We extract it.

`renderWorkloadScript` already exists at main.go:611. Move it; fix the hardcoded `cdc-rows` binary name → `s.Dataset.Seeder`.

- [ ] **Step 1: Write failing tests**

Create `benchmarking/aws/runner/scripts_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"
	"time"
)

// --- renderSeedScript ---

func TestRenderSeedScript_Postgres(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows", InitialRows: 1000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db", "results_bucket": "bucket"}
	script, err := renderSeedScript(s, outs, "stage/cdc-rows")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	if !strings.Contains(script, "POSTGRES_DSN=") {
		t.Errorf("postgres seed script must set POSTGRES_DSN; got:\n%s", script)
	}
	if !strings.Contains(script, "/opt/bench/cdc-rows seed") {
		t.Errorf("postgres seed script must invoke /opt/bench/cdc-rows seed; got:\n%s", script)
	}
}

func TestRenderSeedScript_MySQL(t *testing.T) {
	s := &Scenario{
		Connector: "mysql_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows-mysql", InitialRows: 0},
	}
	outs := map[string]string{"mysql_dsn": "u:p@tcp(h:3306)/db", "results_bucket": "bucket"}
	script, err := renderSeedScript(s, outs, "stage/cdc-rows-mysql")
	if err != nil {
		t.Fatalf("renderSeedScript: %v", err)
	}
	if !strings.Contains(script, "MYSQL_DSN=") {
		t.Errorf("mysql seed script must set MYSQL_DSN; got:\n%s", script)
	}
	if strings.Contains(script, "POSTGRES_DSN=") {
		t.Errorf("mysql seed script must not leak POSTGRES_DSN; got:\n%s", script)
	}
	if !strings.Contains(script, "/opt/bench/cdc-rows-mysql seed") {
		t.Errorf("mysql seed script must invoke /opt/bench/cdc-rows-mysql seed; got:\n%s", script)
	}
}

func TestRenderSeedScript_UnknownConnector(t *testing.T) {
	s := &Scenario{Connector: "unknown_connector", Dataset: DatasetSpec{Seeder: "x"}}
	_, err := renderSeedScript(s, map[string]string{}, "stage/x")
	if err == nil {
		t.Fatal("expected error for unknown connector")
	}
}

// --- combineReset ---

func TestCombineReset_Postgres_DSNForm(t *testing.T) {
	steps := []ResetStep{{SQL: "SELECT 1"}}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	got, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if !strings.Contains(got, "psql ") {
		t.Errorf("postgres reset must use psql; got:\n%s", got)
	}
	if !strings.Contains(got, "SELECT 1") {
		t.Errorf("reset must include SQL; got:\n%s", got)
	}
}

func TestCombineReset_MySQL_HostPortForm(t *testing.T) {
	steps := []ResetStep{{SQL: "TRUNCATE TABLE orders"}}
	outs := map[string]string{
		"mysql_host":     "rpcn-bench-my.xyz.rds.amazonaws.com",
		"mysql_port":     "3306",
		"mysql_user":     "bench",
		"mysql_password": "s3cret",
		"mysql_db":       "benchdb",
	}
	got, err := combineReset("mysql_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if !strings.Contains(got, "mysql ") {
		t.Errorf("mysql reset must invoke mysql CLI; got:\n%s", got)
	}
	if !strings.Contains(got, "-h \"rpcn-bench-my.xyz.rds.amazonaws.com\"") {
		t.Errorf("mysql reset must pass -h flag; got:\n%s", got)
	}
	if !strings.Contains(got, "TRUNCATE TABLE orders") {
		t.Errorf("reset must include SQL; got:\n%s", got)
	}
	if strings.Contains(got, "psql") {
		t.Errorf("mysql reset must not contain psql; got:\n%s", got)
	}
}

func TestCombineReset_EmptySteps(t *testing.T) {
	got, err := combineReset("postgres_cdc", nil, map[string]string{})
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if got != "" {
		t.Errorf("empty reset should produce empty string, got %q", got)
	}
}

func TestCombineReset_BashStepPasses(t *testing.T) {
	// Bash steps should pass through regardless of engine (substitute placeholders).
	steps := []ResetStep{{Bash: "echo ${POSTGRES_DSN}"}}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	got, err := combineReset("postgres_cdc", steps, outs)
	if err != nil {
		t.Fatalf("combineReset: %v", err)
	}
	if !strings.Contains(got, "postgres://u:p@host:5432/db") {
		t.Errorf("bash step must have placeholders substituted; got:\n%s", got)
	}
}

// --- renderWorkloadScript ---

func TestRenderWorkloadScript_Postgres(t *testing.T) {
	s := &Scenario{
		Connector: "postgres_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows"},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute, WriteRatePerSec: 80000},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@host:5432/db"}
	got, err := renderWorkloadScript(s, outs)
	if err != nil {
		t.Fatalf("renderWorkloadScript: %v", err)
	}
	if !strings.Contains(got, "POSTGRES_DSN=") {
		t.Errorf("postgres workload must set POSTGRES_DSN; got:\n%s", got)
	}
	if !strings.Contains(got, "/opt/bench/cdc-rows workload") {
		t.Errorf("postgres workload must invoke cdc-rows; got:\n%s", got)
	}
}

func TestRenderWorkloadScript_MySQL(t *testing.T) {
	s := &Scenario{
		Connector: "mysql_cdc",
		Dataset:   DatasetSpec{Tables: []string{"orders"}, RowSizeBytes: 1200, Seeder: "cdc-rows-mysql"},
		Workload:  &WorkloadSpec{Warmup: 2 * time.Minute, Duration: 15 * time.Minute, WriteRatePerSec: 80000},
	}
	outs := map[string]string{"mysql_dsn": "u:p@tcp(h:3306)/db"}
	got, err := renderWorkloadScript(s, outs)
	if err != nil {
		t.Fatalf("renderWorkloadScript: %v", err)
	}
	if !strings.Contains(got, "MYSQL_DSN=") {
		t.Errorf("mysql workload must set MYSQL_DSN; got:\n%s", got)
	}
	if !strings.Contains(got, "/opt/bench/cdc-rows-mysql workload") {
		t.Errorf("mysql workload must invoke cdc-rows-mysql, not the hardcoded cdc-rows; got:\n%s", got)
	}
}

func TestRenderWorkloadScript_NilWorkload(t *testing.T) {
	s := &Scenario{Connector: "postgres_cdc", Workload: nil}
	got, err := renderWorkloadScript(s, map[string]string{})
	if err != nil {
		t.Fatalf("renderWorkloadScript: %v", err)
	}
	if got != "" {
		t.Errorf("nil workload should produce empty string, got %q", got)
	}
}
```

Imports for `scripts_test.go`:

```go
import (
	"strings"
	"testing"
	"time"
)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/ -run 'TestRenderSeedScript|TestCombineReset|TestRenderWorkloadScript' -v
```

Expected: FAIL — `undefined: renderSeedScript`, and `combineReset` has wrong signature (currently takes `(steps, outs)`, tests expect `(connector, steps, outs)`).

- [ ] **Step 3: Create scripts.go with the three engine-aware renderers**

Create `benchmarking/aws/runner/scripts.go`:

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

// renderSeedScript renders the shell script that runs on the load-gen host to
// pre-seed the source database. The seeder is expected to be staged at
// /opt/bench/<seeder> by the time this runs.
func renderSeedScript(s *Scenario, outs map[string]string, s3Key string) (string, error) {
	es, ok := engineSpecFor(s.Connector)
	if !ok {
		return "", fmt.Errorf("renderSeedScript: connector %q has no engineSpec", s.Connector)
	}
	return fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/%s /opt/bench/%s
chmod +x /opt/bench/%s
%s=%q /opt/bench/%s seed \
  --tables=%s --rows=%d --row-size=%d
`,
		outs["results_bucket"], s3Key, s.Dataset.Seeder, s.Dataset.Seeder,
		es.DSNEnvVar, outs[es.DSNOutputKey], s.Dataset.Seeder,
		strings.Join(s.Dataset.Tables, ","), s.Dataset.InitialRows, s.Dataset.RowSizeBytes,
	), nil
}

// combineReset builds the shell script that runs between every sweep point to
// restore a known state (drop a slot, truncate a table, etc.).
func combineReset(connector string, steps []ResetStep, outs map[string]string) (string, error) {
	if len(steps) == 0 {
		return "", nil
	}
	es, ok := engineSpecFor(connector)
	if !ok {
		return "", fmt.Errorf("combineReset: connector %q has no engineSpec", connector)
	}
	var sb strings.Builder
	sb.WriteString("set -euo pipefail\n")
	for _, st := range steps {
		if st.SQL != "" {
			if es.ResetHostOutputKey != "" {
				// Discrete-flags form (mysql).
				sb.WriteString(fmt.Sprintf(
					`mysql -h %q -P %q -u %q -p%q %q -e %q`+"\n",
					outs[es.ResetHostOutputKey],
					outs[es.ResetPortOutputKey],
					outs[es.ResetUserOutputKey],
					outs[es.ResetPassOutputKey],
					outs[es.ResetDBOutputKey],
					st.SQL,
				))
			} else {
				// DSN form (postgres).
				sb.WriteString(fmt.Sprintf(
					`psql %q -v ON_ERROR_STOP=1 -c %q`+"\n",
					outs[es.DSNOutputKey], st.SQL,
				))
			}
		}
		if st.Bash != "" {
			sb.WriteString(substitutePlaceholders(st.Bash, outs) + "\n")
		}
	}
	return sb.String(), nil
}

// renderWorkloadScript renders the shell script that runs on the load-gen host
// to drive sustained writes while Connect is reading on the runner host.
// Returns "" (no error) when no workload is configured.
func renderWorkloadScript(s *Scenario, outs map[string]string) (string, error) {
	if s.Workload == nil {
		return "", nil
	}
	es, ok := engineSpecFor(s.Connector)
	if !ok {
		return "", fmt.Errorf("renderWorkloadScript: connector %q has no engineSpec", s.Connector)
	}
	totalSec := int((s.Workload.Warmup + s.Workload.Duration).Seconds())
	return fmt.Sprintf(`
set -euo pipefail
%s=%q /opt/bench/%s workload \
  --tables=%s --row-size=%d \
  --rate=%d --duration=%ds
`,
		es.DSNEnvVar, outs[es.DSNOutputKey], s.Dataset.Seeder,
		strings.Join(s.Dataset.Tables, ","),
		s.Dataset.RowSizeBytes,
		s.Workload.WriteRatePerSec,
		totalSec,
	), nil
}
```

- [ ] **Step 4: Delete the old inline copies in main.go**

Edit `benchmarking/aws/runner/main.go`:

**Delete** the existing `combineReset` function (currently around line 592–610).

**Delete** the existing `renderWorkloadScript` function (currently around line 611–627).

**In `runSeeder`** (currently around line 544–591), replace the inline script literal with a call to `renderSeedScript`. The current end of the function looks like:

```go
	key := "stage/" + s.Dataset.Seeder
	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: f,
	}); err != nil {
		return err
	}
	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/%s /opt/bench/%s
chmod +x /opt/bench/%s
POSTGRES_DSN=%q /opt/bench/%s seed \
  --tables=%s --rows=%d --row-size=%d
`,
		bucket, key, s.Dataset.Seeder, s.Dataset.Seeder,
		outs["postgres_dsn"], s.Dataset.Seeder,
		strings.Join(s.Dataset.Tables, ","), s.Dataset.InitialRows, s.Dataset.RowSizeBytes)
	return ssmExec.Run(ctx, outs["load_gen_instance_id"], script, streamingOnLine(os.Stdout, "seed"))
}
```

Replace from `script := fmt.Sprintf(...)` through the closing `}` of the script literal with:

```go
	key := "stage/" + s.Dataset.Seeder
	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: f,
	}); err != nil {
		return err
	}
	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	script, err := renderSeedScript(s, outs, key)
	if err != nil {
		return err
	}
	return ssmExec.Run(ctx, outs["load_gen_instance_id"], script, streamingOnLine(os.Stdout, "seed"))
}
```

**Update the call sites** of `combineReset` and `renderWorkloadScript` elsewhere in `main.go`. Find them with:

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && grep -n "combineReset\|renderWorkloadScript" benchmarking/aws/runner/main.go
```

The current call at around line 230 looks like `reset := combineReset(s.Reset, sharedOuts)`. Change to:

```go
	reset, err := combineReset(s.Connector, s.Reset, sharedOuts)
	if err != nil {
		return err
	}
```

The current call to `renderWorkloadScript(s, sharedOuts)` (or whichever map of outs it reads — check the context) returns a string today. Change to:

```go
	workload, err := renderWorkloadScript(s, sharedOuts)
	if err != nil {
		return err
	}
```

Use the same `outs` map name as is in scope at the call site; both `sharedOuts` and `stackOuts` exist depending on which point in `runBench` you're at. Don't rename them.

- [ ] **Step 5: Run all tests in the runner package**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/ -v 2>&1 | tail -50
```

Expected: PASS. All new script tests pass, all existing scenario/matrix/render/stats/cost/prom/anomalies/summary tests still pass.

- [ ] **Step 6: Verify package builds cleanly**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go build ./benchmarking/aws/runner/...
```

Expected: no output, exit 0.

- [ ] **Step 7: Commit**

```bash
git add benchmarking/aws/runner/scripts.go benchmarking/aws/runner/scripts_test.go benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): extract engine-aware script renderers into scripts.go

renderSeedScript / combineReset / renderWorkloadScript pulled out of
main.go, made engine-aware via engineSpecFor, and unit-tested. Postgres
behavior is unchanged (DSN-form reset, POSTGRES_DSN env); mysql_cdc
will pick the discrete-flags mysql CLI form. Also fixes the hardcoded
'cdc-rows' binary name in the workload script to use s.Dataset.Seeder.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Thread `cache_resources` through `renderPipelineConfig`

**Files:**
- Modify: `benchmarking/aws/runner/main.go` (renderPipelineConfig at ~line 464)
- Test: `benchmarking/aws/runner/scenario_test.go` (or new render_pipeline_test.go)

The renderer currently picks only `s.Pipeline["input"]`. mysql_cdc requires a `cache_resources` block at the Connect-config root. Pass it through if present.

- [ ] **Step 1: Write failing test**

Append to `benchmarking/aws/runner/scenario_test.go` (or create `pipeline_test.go` — same package):

```go
func TestRenderPipelineConfig_PassesCacheResourcesThrough(t *testing.T) {
	tmp := t.TempDir()
	opts := benchOpts{repoRoot: tmp}
	_ = opts // currently unused; included for future need
	s := &Scenario{
		Pipeline: map[string]any{
			"input": map[string]any{
				"mysql_cdc": map[string]any{"dsn": "${MYSQL_DSN}"},
			},
			"cache_resources": []any{
				map[string]any{"label": "bench_checkpoint", "memory": map[string]any{}},
			},
		},
	}
	outs := map[string]string{"mysql_dsn": "u:p@tcp(h:3306)/db"}
	path, err := renderPipelineConfig(s, outs)
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	defer os.Remove(path)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	if !strings.Contains(got, "cache_resources:") {
		t.Errorf("rendered config missing cache_resources block; got:\n%s", got)
	}
	if !strings.Contains(got, "bench_checkpoint") {
		t.Errorf("cache_resources label not threaded through; got:\n%s", got)
	}
	if !strings.Contains(got, "u:p@tcp(h:3306)/db") {
		t.Errorf("MYSQL_DSN placeholder not substituted; got:\n%s", got)
	}
}

func TestRenderPipelineConfig_OmitsCacheResourcesWhenAbsent(t *testing.T) {
	s := &Scenario{
		Pipeline: map[string]any{
			"input": map[string]any{
				"postgres_cdc": map[string]any{"dsn": "${POSTGRES_DSN}"},
			},
		},
	}
	outs := map[string]string{"postgres_dsn": "postgres://u:p@h:5432/db"}
	path, err := renderPipelineConfig(s, outs)
	if err != nil {
		t.Fatalf("renderPipelineConfig: %v", err)
	}
	defer os.Remove(path)
	body, _ := os.ReadFile(path)
	if strings.Contains(string(body), "cache_resources:") {
		t.Errorf("postgres scenario without cache_resources should not have a cache_resources key in rendered config; got:\n%s", body)
	}
}
```

Add `"os"` to the imports if not already present.

- [ ] **Step 2: Run tests, verify they fail**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/ -run TestRenderPipelineConfig -v
```

Expected: FAIL — first test fails because `cache_resources:` is not in the rendered output.

- [ ] **Step 3: Modify renderPipelineConfig**

Edit `benchmarking/aws/runner/main.go`. The function around line 464 currently looks like:

```go
func renderPipelineConfig(s *Scenario, outs map[string]string) (string, error) {
	cfg := map[string]any{
		"http":  map[string]any{"debug_endpoints": true},
		"input": s.Pipeline["input"],
		"output": map[string]any{
			...
```

Add a `cache_resources` pass-through right after `"input": ...`:

```go
func renderPipelineConfig(s *Scenario, outs map[string]string) (string, error) {
	cfg := map[string]any{
		"http":  map[string]any{"debug_endpoints": true},
		"input": s.Pipeline["input"],
		"output": map[string]any{
			"processors": []any{
				map[string]any{
					"benchmark": map[string]any{"interval": "1s", "count_bytes": true},
				},
			},
			"drop": map[string]any{},
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
	// ... rest unchanged
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/ -run TestRenderPipelineConfig -v
```

Expected: PASS (both tests).

- [ ] **Step 5: Run the whole runner package test suite**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/...
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/main.go benchmarking/aws/runner/scenario_test.go
git commit -m "feat(bench/aws): thread cache_resources through to rendered Connect config

mysql_cdc requires a checkpoint_cache resource; the scenario YAML now
declares one in pipeline.cache_resources and the runner surfaces it at
the Connect config root. Postgres scenarios that don't declare one are
unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Install MariaDB client in runner user-data

**Files:**
- Modify: `benchmarking/aws/terraform/shared/runner.tf`

- [ ] **Step 1: Add mariadb1011 to the cloud_init packages**

Edit `benchmarking/aws/terraform/shared/runner.tf`. The current `local.cloud_init` block:

```hcl
locals {
  cloud_init = <<-EOT
    #cloud-config
    package_update: true
    packages:
      - postgresql15
      - jq
    write_files:
      - path: /opt/bench/.gitkeep
        content: ""
    runcmd:
      - mkdir -p /opt/bench
      - chmod 0755 /opt/bench
  EOT
}
```

Add `mariadb1011` to the `packages:` list — it provides the `mysql` CLI used by the mysql_cdc reset script:

```hcl
locals {
  cloud_init = <<-EOT
    #cloud-config
    package_update: true
    packages:
      - postgresql15
      - mariadb1011
      - jq
    write_files:
      - path: /opt/bench/.gitkeep
        content: ""
    runcmd:
      - mkdir -p /opt/bench
      - chmod 0755 /opt/bench
  EOT
}
```

- [ ] **Step 2: Verify Terraform parses**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform/shared && terraform fmt -check
```

Expected: exit 0 (no fmt drift).

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/terraform/shared/runner.tf
git commit -m "build(bench/aws): install MariaDB client in runner+load-gen user-data

Provides the 'mysql' CLI used by the mysql_cdc reset script
(TRUNCATE TABLE orders). AL2023 ships mariadb1011 in the default repo.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: rds-mysql Terraform module

**Files:**
- Create: `benchmarking/aws/terraform/modules/rds-mysql/main.tf`
- Create: `benchmarking/aws/terraform/modules/rds-mysql/variables.tf`
- Create: `benchmarking/aws/terraform/modules/rds-mysql/outputs.tf`

- [ ] **Step 1: Create the directory**

```bash
mkdir -p /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform/modules/rds-mysql
```

- [ ] **Step 2: Write variables.tf**

Create `benchmarking/aws/terraform/modules/rds-mysql/variables.tf`:

```hcl
variable "name_prefix" { type = string }
variable "vpc_id"      { type = string }
variable "subnet_ids" {
  type = list(string)
}
variable "client_sg_ids" {
  type        = list(string)
  description = "SGs allowed to connect on 3306"
}
variable "instance_class" {
  type    = string
  default = "db.r6g.2xlarge"
}
variable "storage_gb" {
  type    = number
  default = 400
}
variable "iops" {
  type    = number
  default = 12000
}
variable "engine_version" {
  type    = string
  default = "8.0.46"
}
variable "db_name" {
  type    = string
  default = "benchdb"
}
variable "master_username" {
  type    = string
  default = "bench"
}
variable "parameters" {
  type = map(string)
  # binlog_format=ROW + binlog_row_image=FULL are required by mysql_cdc.
  # binlog_checksum=NONE keeps the go-mysql client compatible across versions.
  default = {
    binlog_format    = "ROW"
    binlog_row_image = "FULL"
    binlog_checksum  = "NONE"
  }
}
```

- [ ] **Step 3: Write main.tf**

Create `benchmarking/aws/terraform/modules/rds-mysql/main.tf`:

```hcl
resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-my"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "this" {
  name        = "${var.name_prefix}-my-sg"
  description = "Allow MySQL from bench clients"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.client_sg_ids
    content {
      from_port       = 3306
      to_port         = 3306
      protocol        = "tcp"
      security_groups = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_parameter_group" "this" {
  name   = "${var.name_prefix}-my"
  family = "mysql8.0"
  dynamic "parameter" {
    for_each = var.parameters
    content {
      name         = parameter.key
      value        = parameter.value
      apply_method = "pending-reboot"
    }
  }
}

resource "random_password" "master" {
  length  = 20
  special = false
}

resource "aws_db_instance" "this" {
  identifier             = "${var.name_prefix}-my"
  engine                 = "mysql"
  engine_version         = var.engine_version
  instance_class         = var.instance_class
  allocated_storage      = var.storage_gb
  storage_type           = "gp3"
  iops                   = var.iops
  db_name                = var.db_name
  username               = var.master_username
  password               = random_password.master.result
  parameter_group_name   = aws_db_parameter_group.this.name
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.this.id]
  skip_final_snapshot    = true
  deletion_protection    = false
  publicly_accessible    = false
  apply_immediately      = true

  # CRITICAL: RDS purges binlog immediately if backups are off. CDC needs the
  # binlog to be retained long enough for the connector to read it. 1 day is
  # the minimum that keeps binlog around between writes and reads.
  backup_retention_period = 1
}
```

- [ ] **Step 4: Write outputs.tf**

Create `benchmarking/aws/terraform/modules/rds-mysql/outputs.tf`:

```hcl
output "mysql_dsn" {
  # go-sql-driver/mysql DSN: user:pass@tcp(host:port)/db?params
  # parseTime=true maps DATETIME → time.Time at the driver layer.
  # tls=skip-verify because the RDS-internal CA isn't in the runner image.
  value     = "${var.master_username}:${random_password.master.result}@tcp(${aws_db_instance.this.address}:3306)/${var.db_name}?parseTime=true&tls=skip-verify"
  sensitive = true
}
output "mysql_endpoint" { value = aws_db_instance.this.address }
output "mysql_host"     { value = aws_db_instance.this.address }
output "mysql_port"     { value = "3306" }
output "mysql_user"     { value = var.master_username }
output "mysql_db"       { value = var.db_name }
output "mysql_password" {
  value     = random_password.master.result
  sensitive = true
}
```

Note: `mysql_port` is a string (`"3306"`) so it slots into `outs map[string]string` in the runner without conversion. Match the postgres module which exposes a similar `_endpoint`/`_dsn` shape.

- [ ] **Step 5: Verify Terraform formats and parses**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform/modules/rds-mysql && terraform fmt -check && terraform init -backend=false && terraform validate
```

Expected: `terraform init -backend=false` succeeds (downloads providers), `terraform validate` reports `Success!`.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/terraform/modules/rds-mysql/
git commit -m "feat(bench/aws/mysql): rds-mysql Terraform module

Mirrors rds-postgres with engine='mysql' and the binlog parameter group
needed by mysql_cdc (binlog_format=ROW, binlog_row_image=FULL,
binlog_checksum=NONE). Sets backup_retention_period=1 — required for
RDS to retain binlog past the moment it's written. Outputs the
go-sql-driver DSN plus discrete host/port/user/pass/db pieces for the
mysql CLI used in reset scripts.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: mysql Terraform stack

**Files:**
- Create: `benchmarking/aws/terraform/stacks/mysql/main.tf`
- Create: `benchmarking/aws/terraform/stacks/mysql/variables.tf`
- Create: `benchmarking/aws/terraform/stacks/mysql/outputs.tf`

- [ ] **Step 1: Create the directory**

```bash
mkdir -p /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform/stacks/mysql
```

- [ ] **Step 2: Write variables.tf**

Create `benchmarking/aws/terraform/stacks/mysql/variables.tf`. Copy the existing postgres stack's variables.tf to start, then s/postgres/mysql for the Stack tag and any defaults:

```bash
cp /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform/stacks/postgres/variables.tf /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform/stacks/mysql/variables.tf
```

Confirm the file matches the postgres variables (region, instance_class, storage_gb, iops, parameters). The mysql stack accepts the same shape — these get passed through to the rds-mysql module. If the postgres `parameters` default has Postgres-specific keys, override with mysql defaults:

Read the file and ensure the `parameters` variable default is:

```hcl
variable "parameters" {
  type    = map(string)
  default = {
    binlog_format    = "ROW"
    binlog_row_image = "FULL"
    binlog_checksum  = "NONE"
  }
}
```

If the postgres file's `parameters` default has Postgres keys (e.g. `rds.logical_replication`), replace just the default — leave the variable declaration shape identical.

- [ ] **Step 3: Write main.tf**

Create `benchmarking/aws/terraform/stacks/mysql/main.tf`:

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.70" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "redpanda-connect-bench"
      Stack     = "mysql"
      ManagedBy = "terraform"
    }
  }
}

data "terraform_remote_state" "shared" {
  backend = "s3"
  config = {
    bucket = "redpanda-connect-bench-tfstate"
    region = var.region
    key    = "shared/terraform.tfstate"
  }
}

module "rds" {
  source         = "../../modules/rds-mysql"
  name_prefix    = "rpcn-bench-my"
  vpc_id         = data.terraform_remote_state.shared.outputs.vpc_id
  subnet_ids     = data.terraform_remote_state.shared.outputs.private_subnet_ids
  client_sg_ids = [
    data.terraform_remote_state.shared.outputs.runner_sg_id,
    data.terraform_remote_state.shared.outputs.load_gen_sg_id,
  ]
  instance_class = var.instance_class
  storage_gb     = var.storage_gb
  iops           = var.iops
  parameters     = var.parameters
}
```

- [ ] **Step 4: Write outputs.tf**

Create `benchmarking/aws/terraform/stacks/mysql/outputs.tf`:

```hcl
output "mysql_dsn"      { value = module.rds.mysql_dsn      sensitive = true }
output "mysql_endpoint" { value = module.rds.mysql_endpoint }
output "mysql_host"     { value = module.rds.mysql_host }
output "mysql_port"     { value = module.rds.mysql_port }
output "mysql_user"     { value = module.rds.mysql_user }
output "mysql_db"       { value = module.rds.mysql_db }
output "mysql_password" { value = module.rds.mysql_password sensitive = true }
```

- [ ] **Step 5: Verify Terraform formats and parses**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform/stacks/mysql && terraform fmt -check && terraform init -backend=false && terraform validate
```

Expected: `terraform validate` reports `Success!`. (init may complain about missing `key=...` in backend config; that's fine for the `-backend=false` form.)

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/terraform/stacks/mysql/
git commit -m "feat(bench/aws/mysql): mysql stack composing shared + rds-mysql

Mirrors stacks/postgres. Stack tag 'mysql' for cost-explorer breakdown.
Reads shared VPC/SGs via remote-state and wires them into the rds-mysql
module. Exposes the six mysql_* outputs the runner needs for the
engineSpec lookup.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: cdc-rows-mysql seeder

**Files:**
- Create: `benchmarking/aws/seeders/cdc-rows-mysql/main.go`
- Create: `benchmarking/aws/seeders/cdc-rows-mysql/sql.go`
- Modify: `go.mod`, `go.sum` if `go-sql-driver/mysql` isn't already a direct dependency.

- [ ] **Step 1: Create the directory**

```bash
mkdir -p /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/seeders/cdc-rows-mysql
```

- [ ] **Step 2: Write main.go**

Create `benchmarking/aws/seeders/cdc-rows-mysql/main.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cdc-rows-mysql {seed|workload} [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	switch cmd {
	case "seed":
		fs := flag.NewFlagSet("seed", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated table list")
		rows := fs.Int64("rows", 1_000_000, "rows per table")
		rowSize := fs.Int("row-size", 1200, "approximate row size in bytes")
		_ = fs.Parse(os.Args[2:])
		if err := seed(context.Background(), strings.Split(*tables, ","), *rows, *rowSize); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			os.Exit(1)
		}
	case "workload":
		fs := flag.NewFlagSet("workload", flag.ExitOnError)
		tables := fs.String("tables", "orders", "comma-separated table list")
		rowSize := fs.Int("row-size", 1200, "approximate row size in bytes")
		rate := fs.Int("rate", 5000, "writes per second total across tables")
		dur := fs.Duration("duration", 15*time.Minute, "total duration")
		_ = fs.Parse(os.Args[2:])
		if err := workload(context.Background(), strings.Split(*tables, ","), *rowSize, *rate, *dur); err != nil {
			fmt.Fprintln(os.Stderr, "workload:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", cmd)
		os.Exit(2)
	}
}
```

- [ ] **Step 3: Write sql.go**

Create `benchmarking/aws/seeders/cdc-rows-mysql/sql.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func openDB(maxConns int) (*sql.DB, error) {
	db, err := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

func seed(ctx context.Context, tables []string, rows int64, rowSize int) error {
	db, err := openDB(16)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, table := range tables {
		if err := ensureTable(ctx, db, table, rowSize); err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(tables))
	for _, table := range tables {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			errCh <- bulkInsert(ctx, db, t, rows, rowSize)
		}(table)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func ensureTable(ctx context.Context, db *sql.DB, table string, rowSize int) error {
	stmts := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf(`CREATE TABLE %s (
			id          BIGINT AUTO_INCREMENT PRIMARY KEY,
			created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			payload     TEXT NOT NULL
		) ENGINE=InnoDB`, table),
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	_ = rowSize
	return nil
}

func bulkInsert(ctx context.Context, db *sql.DB, table string, rows int64, rowSize int) error {
	const workers = 16
	rowsPerWorker := rows / workers
	if rowsPerWorker == 0 {
		// Allow rows=0 (scenario.dataset.initial_rows: 0): ensureTable already
		// ran, so the table exists but stays empty.
		return nil
	}
	payload := randomPayload(rowSize)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const batchSize = 1000
			batch := strings.Repeat("(NOW(6),?),", batchSize)
			batch = strings.TrimSuffix(batch, ",")
			stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, batch)
			args := make([]any, batchSize)
			for i := range args {
				args[i] = payload
			}
			done := int64(0)
			for done < rowsPerWorker {
				if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
					errCh <- err
					return
				}
				done += int64(batchSize)
			}
			errCh <- nil
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	fmt.Printf("seeded %d rows into %s in %s\n", rows, table, time.Since(start))
	return nil
}

func workload(ctx context.Context, tables []string, rowSize, rate int, dur time.Duration) error {
	// 8 workers, each writing rate/8/10 rows per 100ms tick. Mirrors the
	// postgres seeder's parallelism so the producer can match the scenario's
	// write_rate_per_sec at 80K+ writes/sec.
	const workers = 8
	db, err := openDB(workers)
	if err != nil {
		return err
	}
	defer db.Close()

	perWorkerPer100ms := rate / workers / 10
	if perWorkerPer100ms < 1 {
		perWorkerPer100ms = 1
	}
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerIdx := w
		go func() {
			defer wg.Done()
			payload := randomPayload(rowSize)
			batch := strings.Repeat("(NOW(6),?),", perWorkerPer100ms)
			batch = strings.TrimSuffix(batch, ",")
			args := make([]any, perWorkerPer100ms)
			for i := range args {
				args[i] = payload
			}
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			tIdx := workerIdx
			for {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case <-ticker.C:
					if time.Now().After(deadline) {
						errCh <- nil
						return
					}
					table := tables[tIdx%len(tables)]
					tIdx++
					stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, batch)
					if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			return err
		}
	}
	return nil
}

func randomPayload(size int) string {
	b := make([]byte, (size*3)/4+1)
	_, _ = rand.Read(b)
	s := base64.StdEncoding.EncodeToString(b)
	if len(s) > size {
		s = s[:size]
	}
	return s
}
```

- [ ] **Step 4: Verify go-sql-driver/mysql is available**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && grep go-sql-driver/mysql go.mod
```

Expected: a line like `github.com/go-sql-driver/mysql v1.x.x` is present (transitively pulled by the mysql_cdc connector).

If absent, run `go mod tidy` after the next build step to add it.

- [ ] **Step 5: Build the seeder for linux/arm64 (the runner target)**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/cdc-rows-mysql ./benchmarking/aws/seeders/cdc-rows-mysql
ls -la /tmp/cdc-rows-mysql
```

Expected: a ~10MB ARM64 binary at /tmp/cdc-rows-mysql.

- [ ] **Step 6: Also build for the local platform (sanity check)**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go build ./benchmarking/aws/seeders/cdc-rows-mysql
```

Expected: no errors. (`go build` without `-o` builds the package and discards the binary.)

- [ ] **Step 7: If go.mod was updated, commit those changes too**

```bash
git status
```

If go.mod / go.sum changed, include them in the commit.

- [ ] **Step 8: Commit**

```bash
git add benchmarking/aws/seeders/cdc-rows-mysql/ go.mod go.sum
git commit -m "feat(bench/aws/mysql): cdc-rows-mysql seeder

database/sql + go-sql-driver/mysql variant of cdc-rows. Same CLI shape
(seed/workload subcommands), same 8-worker workload pattern. Adapts
SQL flavor: BIGINT AUTO_INCREMENT, DATETIME(6), NOW(6), '?' positional
placeholders. MYSQL_DSN env var.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: mysql scenario YAML

**Files:**
- Create: `benchmarking/aws/scenarios/mysql/orders-cdc.yaml`

- [ ] **Step 1: Create the directory**

```bash
mkdir -p /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/scenarios/mysql
```

- [ ] **Step 2: Write the scenario**

Create `benchmarking/aws/scenarios/mysql/orders-cdc.yaml`:

```yaml
name: mysql-orders-cdc
description: |
  Stream changes from a high-write MySQL orders table (target 80K writes/sec
  ≈ 96 MB/s) so the mysql_cdc input — not the producer — is the bottleneck.
  TRUNCATE between sweep points keeps the table size bounded (no Trap 3).

connector: mysql_cdc
stack: mysql

infra:
  source:
    instance_class: db.r6g.2xlarge
    storage_gb: 400
    iops: 12000
    # parameters left at module defaults: binlog_format=ROW, binlog_row_image=FULL,
    # binlog_checksum=NONE. backup_retention_period=1 is set in the module itself
    # because it's a required-for-CDC constant, not a per-scenario knob.
  runner:
    instance_type: c8g.4xlarge

dataset:
  initial_rows: 0          # TRUNCATE-before-every-point makes a seed pointless;
                           # ensureTable still runs to CREATE the table.
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows-mysql

workload:
  write_rate_per_sec: 80000
  duration: 15m
  warmup: 2m

pipeline:
  cache_resources:
    - label: bench_checkpoint
      memory: {}
  input:
    mysql_cdc:
      flavor: mysql
      dsn: ${MYSQL_DSN}
      tls:
        skip_cert_verify: true   # RDS-internal CA isn't in the runner image;
                                  # mysql_cdc uses NewTLSField (no `enabled` toggle).
      stream_snapshot: false
      tables: [orders]
      checkpoint_cache: bench_checkpoint
      checkpoint_key: mysql_bench
      batching:
        count: 5000
        period: 1s

matrix:
  cpu_points: [1, 2, 4, 8]

reset:
  - sql: "TRUNCATE TABLE orders"
```

- [ ] **Step 3: Validate the scenario via the runner**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws && task aws:validate scenario=mysql/orders-cdc
```

Expected:
```
scenario mysql-orders-cdc OK (4 cpu points, runner c8g.4xlarge)
```

If the runner reports `connector "mysql_cdc" has no engineSpec entry`, Task 1 is incomplete — go back and fix.

- [ ] **Step 4: Confirm the rendered Connect pipeline config is well-formed**

A quick smoke that doesn't require AWS: write a tiny throwaway Go test that exercises `renderPipelineConfig` on this scenario. Skip if Task 3's test already covers the cache_resources path — it does. So no extra step here.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/scenarios/mysql/orders-cdc.yaml
git commit -m "feat(bench/aws/mysql): orders-cdc scenario

80K writes/sec → 96 MB/s mirroring the postgres baseline.
initial_rows: 0 + TRUNCATE between points kills Trap 3.
memory checkpoint_cache keeps reset trivial (Connect restart resets it).
tls: { skip_cert_verify: true } because RDS-internal CA isn't on the runner.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: docs/benchmark-results/mysql.md skeleton

**Files:**
- Create: `docs/benchmark-results/mysql.md`

- [ ] **Step 1: Write the header**

Create `docs/benchmark-results/mysql.md`:

```markdown
# MySQL Benchmark Results

This file holds the AWS-bench results for `mysql_cdc`. Per-scenario rows are
appended by `task aws:bench` (see `benchmarking/aws/README.md`). For local
laptop-Docker bench results see [`mysql-cdc.md`](./mysql-cdc.md).

---
```

- [ ] **Step 2: Commit**

```bash
git add docs/benchmark-results/mysql.md
git commit -m "docs(bench/aws/mysql): mysql.md skeleton for AWS bench rows

The runner appends per-scenario sections to docs/benchmark-results/<stack>.md.
This file exists in git so the very first append-run has a non-empty parent
file. Local laptop bench results live in mysql-cdc.md (unchanged).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Update benchmarking/aws/README.md

**Files:**
- Modify: `benchmarking/aws/README.md`

- [ ] **Step 1: Locate the Postgres section in the README**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && grep -n "^## " benchmarking/aws/README.md
```

Find the line numbers of the `## Postgres` (or similar) section heading and whichever heading comes after it. The new MySQL section goes between them. If no per-connector section exists yet, append the MySQL section at the end of the file, before any footer/links section.

- [ ] **Step 2: Insert the MySQL section**

Insert at the identified location:

```markdown
## MySQL (`mysql_cdc`)

**Scenario:** `mysql/orders-cdc` — 80K writes/sec sustained workload, 4-point vCPU sweep, TRUNCATE between points.

**Run:**

```
cd benchmarking/aws && \
  unset AWS_PROFILE && \
  aws-vault exec AWSAdministratorAccess-605419575229 -- \
    env REDPANDA_LICENSE_FILEPATH=/path/to/rpcn.license \
    task aws:bench scenario=mysql/orders-cdc
```

**RDS-MySQL gotchas:**

- `backup_retention_period=1` is required. RDS purges binlog immediately if backups are off; mysql_cdc then has nothing to read. The `rds-mysql` module hardcodes this — do NOT set it to 0.
- `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_checksum=NONE` are pre-set in the module's parameter group. Override via `var.parameters` if needed.
- mysql_cdc's `tls:` block uses `service.NewTLSField` — there is NO `enabled:` toggle (same gotcha as postgres_cdc). Always include `tls: { skip_cert_verify: true }` for RDS.
- The reset CLI uses `mysql` (provided by `mariadb1011` in the runner's user-data). If a future AL2023 image drops that package, the runner.tf user-data needs an update.

**Adding a new engine in the future:**

Add an entry to `engineSpecs` in `benchmarking/aws/runner/scenario.go`. The map keys are connector names (`postgres_cdc`, `mysql_cdc`, ...) and values carry the terraform output keys and env var names that the seed/reset/workload script renderers need. Engines whose CLI accepts a DSN-URL (psql) leave `ResetHostOutputKey` empty; engines whose CLI wants discrete flags (mysql) populate all five `Reset*OutputKey` fields. No other file changes are needed for the runner side.
```

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws/mysql): document mysql scenario, RDS gotchas, engineSpec extension

backup_retention_period=1 is the non-obvious gotcha worth documenting up
front. Also documents how to add a future connector via the
engineSpecs map (one-entry change, no switch-statement edits).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Full-package regression test before AWS smoke

**Files:** none modified.

- [ ] **Step 1: Run the full runner package test suite**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && go test ./benchmarking/aws/runner/... -count=1
```

Expected: PASS, all tests.

- [ ] **Step 2: Run terraform fmt + validate on everything**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws/terraform && \
  for d in shared stacks/postgres stacks/mysql modules/rds-postgres modules/rds-mysql; do
    echo "=== $d ==="
    (cd "$d" && terraform fmt -check && terraform init -backend=false >/dev/null 2>&1 && terraform validate)
  done
```

Expected: every dir reports `Success!`.

- [ ] **Step 3: Validate both scenarios**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws && \
  task aws:validate scenario=postgres/orders-cdc && \
  task aws:validate scenario=mysql/orders-cdc
```

Expected: both report OK.

- [ ] **Step 4: If everything passes, no commit needed — proceed to Task 12**

---

### Task 12: Live AWS smoke run (operator)

**Files:** none. This is an operator step that requires AWS access, license file, and ~90 min of clock time.

This step is NOT executed by an agent. It is documented here so the operator knows the exact command and success criteria. Do NOT skip it — without this run, mysql is not actually verified.

- [ ] **Step 1: Confirm preconditions**

- License file at `/Users/prakhar.garg/Documents/connect_prakhar/rpcn.license` (repo root; gitignored).
- `aws-vault` configured with profile `AWSAdministratorAccess-605419575229`.
- Working directory clean (`git status` shows no uncommitted changes — the bench appends to `docs/benchmark-results/mysql.md` automatically and that's the change we want to land).

- [ ] **Step 2: Run the scenario**

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar/benchmarking/aws && \
  unset AWS_PROFILE && \
  aws-vault exec AWSAdministratorAccess-605419575229 -- \
    env REDPANDA_LICENSE_FILEPATH=/Users/prakhar.garg/Documents/connect_prakhar/rpcn.license \
    task aws:bench scenario=mysql/orders-cdc
```

Expected wall-clock: ~90 minutes (4 sweep points × ~17 min each + setup/teardown).

Expected cost: ~$3.

- [ ] **Step 3: Verify success criteria**

After the run completes:

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar && cat docs/benchmark-results/mysql.md
```

Expected: A new `## AWS — orders-cdc — YYYY-MM-DD` section appended. All four cpu_points (1, 2, 4, 8) have non-zero median MB/s. (Compare against the postgres `orders-cdc` row's shape.)

```bash
grep -A20 "## AWS Bench Results" docs/benchmark-results/SUMMARY.md
```

Expected: A new row for mysql/orders-cdc.

- [ ] **Step 4: Handle failure modes**

If point 1 returns 0 samples → check the per-point S3 log (`s3://results_bucket/runs/<session>/sweep-1.log`). Common causes:
- `mysql` CLI missing on runner → Task 4 user-data wasn't applied or `mariadb1011` package doesn't exist on the AMI's AL2023 version. Workaround: change package name to `mariadb105` and re-run.
- mysql_cdc connection failure → check the TLS block, the DSN URL-vs-tcp() shape, the binlog params.
- Connect license not present → check the staging step in main.go.

If all 4 points produce identical median MB/s ≈ 96 → workload-bound (Trap 1). Bump `write_rate_per_sec` to 150000 and re-run.

If point 4 (8 vCPU) returns 0 → producer stalled (Trap 3). Should NOT happen with TRUNCATE-between-points, but if it does, investigate the reset SQL execution.

- [ ] **Step 5: Commit the published result**

```bash
git add docs/benchmark-results/mysql.md docs/benchmark-results/SUMMARY.md
git commit -m "docs(bench/aws/mysql): publish first orders-cdc result

[fill in the median MB/s per point from the run]

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

Update memory ([[bench-framework-state]], [[bench-bucket-3-mysql-todo]]) after the run lands.

---

## Acceptance

- [ ] All 11 code/TF/docs tasks committed.
- [ ] `go test ./benchmarking/aws/runner/...` passes.
- [ ] `task aws:validate scenario=postgres/orders-cdc` passes (no regression).
- [ ] `task aws:validate scenario=mysql/orders-cdc` passes.
- [ ] Task 12 live AWS smoke: 4 non-zero data points landed in `docs/benchmark-results/mysql.md`; SUMMARY.md auto-refreshed.

Self-review notes:

- **Spec coverage:** every section in the spec maps to a task. Runner generalisation = T1+T2. Pipeline-config rendering = T3. Runner-mysql-client install = T4. rds-mysql TF module = T5. mysql stack = T6. Seeder = T7. Scenario = T8. Empty results doc = T9. README = T10. Validate gate = T11. AWS smoke = T12.
- **Placeholder scan:** zero "TBD" / "TODO" / "add error handling" / "similar to Task N" — code blocks repeated where needed.
- **Type consistency:** `engineSpec` shape used in T1 matches the field names referenced in T2's `combineReset`, `renderSeedScript`, `renderWorkloadScript`. `engineSpecFor` returns `(engineSpec, bool)` consistently. `ResetStep`, `Scenario`, `DatasetSpec`, `WorkloadSpec` field names match the existing scenario.go types.
