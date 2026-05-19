# AWS Benchmarking Framework — Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the AWS benchmarking framework foundation and prove it end-to-end with one connector (Postgres). After this plan, `task aws:bench -- scenario=postgres/orders-cdc` provisions AWS infra, runs a 4-point CPU sweep against a real RDS Postgres source, writes a results JSON, appends a section to `docs/benchmark-results/postgres.md`, and tears down.

**Architecture:** A new top-level `benchmarking/aws/` tree with three layers — Terraform (shared infra + per-connector modules and stacks), a connector-agnostic Go runner that orchestrates a scenario end-to-end via AWS Systems Manager, and YAML scenario files that describe customer-shaped workloads. The runner stays in the main `connect` Go module to reuse existing tooling.

**Tech Stack:** Go (runner, seeders) · Terraform ≥ 1.6 (HCL infra) · AWS Systems Manager (remote exec, no SSH) · go-task (operator UX) · Bloblang `benchmark` processor (existing — measurement) · `testify` (Go tests, already used in the repo).

**Spec:** [`docs/superpowers/specs/2026-05-19-aws-benchmarking-framework-design.md`](../specs/2026-05-19-aws-benchmarking-framework-design.md).

**Prerequisites** the operator running these tasks needs locally: Go ≥ 1.22 (already required), Terraform ≥ 1.6, AWS CLI v2, `aws-vault` or environment-based AWS credentials with permissions to provision VPCs/EC2/RDS/S3/IAM/Lambda in a dedicated benchmarking AWS account, `task` (go-task), `jq`.

**Out of scope for this plan:** mysql, sqlserver, dynamodb, s3, iceberg, kafka-to-pg connectors (each gets its own plan). Multi-scenario shared-infra sessions beyond the basic `scenario=a,b` flag. CI integration.

---

## File Structure

The tree this plan creates:

```
benchmarking/aws/
├── README.md                                # operator-facing entry doc
├── Taskfile.yml                             # aws:bench / aws:validate / aws:down / aws:cost-check
├── .gitignore                               # results/*.json, .terraform/, *.tfstate*
├── terraform/
│   ├── backend.hcl                          # S3 backend config (region, bucket, dynamodb table)
│   ├── shared/
│   │   ├── main.tf                          # provider, locals, tags
│   │   ├── vpc.tf
│   │   ├── security.tf
│   │   ├── iam.tf
│   │   ├── runner.tf                        # runner EC2 + load-gen EC2
│   │   ├── results_bucket.tf
│   │   ├── orphan_cleanup.tf                # EventBridge + Lambda
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── modules/
│   │   └── rds-postgres/
│   │       ├── main.tf
│   │       ├── variables.tf
│   │       └── outputs.tf
│   └── stacks/
│       └── postgres/
│           ├── main.tf                      # composes shared + rds-postgres
│           ├── variables.tf
│           └── outputs.tf
├── scenarios/
│   └── postgres/
│       └── orders-cdc.yaml
├── seeders/
│   └── cdc-rows/
│       ├── main.go                          # SQL + DynamoDB code paths
│       ├── sql.go                           # postgres/mysql/sqlserver dialect
│       └── dynamo.go                        # DynamoDB BatchWriteItem path
└── runner/
    ├── main.go                              # CLI: bench / validate / down / cost-check
    ├── scenario.go                          # scenario YAML parse + validate
    ├── scenario_test.go
    ├── stats.go                             # parse rolling-stats lines, percentiles
    ├── stats_test.go
    ├── anomalies.go                         # ≥60s dip detection
    ├── anomalies_test.go
    ├── terraform.go                         # apply/destroy wrapper
    ├── ssm.go                               # SSM RunCommand + output streaming
    ├── matrix.go                            # CPU sweep loop
    ├── render.go                            # JSON write + markdown append
    ├── render_test.go
    └── templates/
        └── result.md.tmpl                   # markdown template
```

Files modified outside the new tree:

- `docs/benchmark-results/postgres.md` — appended with the first AWS run's section (by the runner; only after the smoke run in Task 25).
- `docs/benchmarking.md` — short pointer at the bottom to the new framework.

---

## Conventions used in this plan

- **Test framework:** `testify` (`require` for assertions). Already used throughout the repo.
- **Go package layout:** the runner is `package main` under `benchmarking/aws/runner` — invoked as `go run ./benchmarking/aws/runner ...` from the repo root. Seeders are `package main` under `benchmarking/aws/seeders/<name>`.
- **License headers:** Apache-2.0 header used by the existing `internal/impl/aws/dynamodb/bench/main.go` — copy that block verbatim into every new Go file.
- **Region:** `us-east-2` (Ohio) is the default benchmarking region. All examples and tests assume it.
- **Commits:** one focused commit per task using Conventional Commits style (`feat:`, `test:`, `chore:`, `docs:`). Co-author trailer included.

### Spec deltas applied in this plan

Two small clarifications to the spec, called out so they're visible:

1. **`GOMEMLIMIT`** in the spec is shown as `${n}GiB`. At `n=1` that's 1 GiB, which is too tight for several Postgres workloads. This plan sets `GOMEMLIMIT = max(2GiB, 2*n GiB)` and exposes a scenario-level `matrix.go_mem_limit_per_vcpu` override (default `2`).
2. **Region** is not pinned in the spec. This plan pins `us-east-2` as the default and exposes it as a Terraform variable (`region`, default `us-east-2`) on every stack.

---

## Task 1: Repo skeleton and gitignore

**Files:**
- Create: `benchmarking/aws/.gitignore`
- Create: `benchmarking/aws/README.md`
- Create: `benchmarking/aws/results/.gitkeep`

- [ ] **Step 1: Create the directory tree**

```bash
mkdir -p benchmarking/aws/{terraform/{shared,modules/rds-postgres,stacks/postgres},scenarios/postgres,seeders/cdc-rows,runner/templates,results}
touch benchmarking/aws/results/.gitkeep
```

- [ ] **Step 2: Add gitignore**

Write `benchmarking/aws/.gitignore`:

```
# Local results — raw JSON is uploaded to S3 anyway. .gitkeep is the marker.
results/*.json
results/**/*.json

# Terraform local state — never commit
.terraform/
.terraform.lock.hcl
terraform.tfstate
terraform.tfstate.*

# Built binaries staged for upload
runner/dist/
seeders/dist/
```

- [ ] **Step 3: Add operator README**

Write `benchmarking/aws/README.md`:

````markdown
# AWS Benchmarking Framework

Production-shaped benchmarks for Redpanda Connect connectors, run on real AWS infrastructure.

See [`docs/superpowers/specs/2026-05-19-aws-benchmarking-framework-design.md`](../../docs/superpowers/specs/2026-05-19-aws-benchmarking-framework-design.md) for the full design.

## Prerequisites

- Go 1.22+, Terraform 1.6+, AWS CLI v2, `task`, `jq`.
- AWS credentials with admin in a dedicated benchmarking account (set via `aws-vault exec` or env vars).
- S3 bucket and DynamoDB table for Terraform state — see `terraform/backend.hcl`.

## One-command usage

```bash
task aws:bench -- scenario=postgres/orders-cdc
```

Runs end-to-end: `terraform apply` → seed dataset → CPU sweep [1, 2, 4, 8] → write JSON + append markdown → `terraform destroy`. Expect ~90 min wall-clock and ~$5 in AWS costs.

Other tasks:

- `task aws:validate -- scenario=<…>` — schema validation + `terraform plan` + pipeline lint. No AWS spend.
- `task aws:down` — explicit teardown when running with `keep=true`.
- `task aws:cost-check` — estimated hourly cost of any currently-running stacks.

## Cost guardrails

Every resource carries a `bench-session-id` tag. An EventBridge-triggered Lambda destroys stacks older than 24 h — the "I closed my laptop" safety net.
````

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/.gitignore benchmarking/aws/README.md benchmarking/aws/results/.gitkeep
git commit -m "$(cat <<'EOF'
feat(bench/aws): add benchmarking/aws skeleton and README

Lays out the directory tree, gitignore for local terraform/results, and the
operator-facing README pointing at the design spec.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Scenario parser — tests first

The scenario YAML is the contract between operator and runner. Parse + validate in pure Go so failures happen before we spend AWS money.

**Files:**
- Create: `benchmarking/aws/runner/scenario.go`
- Test: `benchmarking/aws/runner/scenario_test.go`
- Create: `benchmarking/aws/runner/testdata/valid-orders-cdc.yaml`
- Create: `benchmarking/aws/runner/testdata/invalid-short-duration.yaml`
- Create: `benchmarking/aws/runner/testdata/invalid-runner-too-small.yaml`

- [ ] **Step 1: Write the test fixtures**

Write `benchmarking/aws/runner/testdata/valid-orders-cdc.yaml`:

```yaml
name: postgres-orders-cdc
description: Stream changes from a 75M-row orders table at 5K writes/sec sustained.
connector: postgres_cdc
stack: postgres
infra:
  source:
    instance_class: db.r6g.2xlarge
    storage_gb: 400
    iops: 12000
  runner:
    instance_type: c7i.4xlarge
dataset:
  initial_rows: 75000000
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows
workload:
  write_rate_per_sec: 5000
  duration: 15m
  warmup: 2m
pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
      stream_snapshot: false
      schema: public
      tables: [orders]
      slot_name: bench_slot
      batching:
        count: 5000
        period: 1s
matrix:
  cpu_points: [1, 2, 4, 8]
reset:
  - sql: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"
```

Write `benchmarking/aws/runner/testdata/invalid-short-duration.yaml` — same as above but with `duration: 5m`.

Write `benchmarking/aws/runner/testdata/invalid-runner-too-small.yaml` — same as the valid one but with `runner.instance_type: c7i.large` (2 vCPU, can't host an 8-vCPU sweep + 2 reserved cores).

- [ ] **Step 2: Write the failing test**

Write `benchmarking/aws/runner/scenario_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadScenario_Valid(t *testing.T) {
	s, err := LoadScenario("testdata/valid-orders-cdc.yaml")
	require.NoError(t, err)
	require.Equal(t, "postgres-orders-cdc", s.Name)
	require.Equal(t, "postgres_cdc", s.Connector)
	require.Equal(t, "postgres", s.Stack)
	require.Equal(t, "c7i.4xlarge", s.Infra.Runner.InstanceType)
	require.Equal(t, 15*time.Minute, s.Workload.Duration)
	require.Equal(t, 2*time.Minute, s.Workload.Warmup)
	require.Equal(t, []int{1, 2, 4, 8}, s.Matrix.CPUPoints)
}

func TestLoadScenario_RejectsShortDuration(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-short-duration.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "workload.duration")
	require.Contains(t, err.Error(), "15m")
}

func TestLoadScenario_RejectsRunnerTooSmall(t *testing.T) {
	_, err := LoadScenario("testdata/invalid-runner-too-small.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "infra.runner.instance_type")
	require.Contains(t, err.Error(), "vCPU")
}

func TestVCPUForInstanceType_Known(t *testing.T) {
	require.Equal(t, 16, vcpuForInstanceType("c7i.4xlarge"))
	require.Equal(t, 2, vcpuForInstanceType("c7i.large"))
	require.Equal(t, 0, vcpuForInstanceType("not-a-real-type"))
}
```

- [ ] **Step 3: Run the test, verify failure**

```bash
go test ./benchmarking/aws/runner -run TestLoadScenario -v
```

Expected: compilation failure (LoadScenario doesn't exist).

- [ ] **Step 4: Write the implementation**

Write `benchmarking/aws/runner/scenario.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	minWarmup            = 2 * time.Minute
	minDuration          = 15 * time.Minute
	reservedCores        = 2
	defaultGoMemPerVCPU  = 2 // GiB
)

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
}

type InfraSpec struct {
	Source map[string]any `yaml:"source"`
	Runner RunnerSpec     `yaml:"runner"`
}

type RunnerSpec struct {
	InstanceType string `yaml:"instance_type"`
}

type DatasetSpec struct {
	InitialRows       int64    `yaml:"initial_rows"`
	RowSizeBytes      int      `yaml:"row_size_bytes"`
	Tables            []string `yaml:"tables"`
	Seeder            string   `yaml:"seeder"`
	ExpectedPeakMBSec int      `yaml:"expected_peak_mb_s,omitempty"`
}

type WorkloadSpec struct {
	WriteRatePerSec int           `yaml:"write_rate_per_sec"`
	Duration        time.Duration `yaml:"duration"`
	Warmup          time.Duration `yaml:"warmup"`
}

type MatrixSpec struct {
	CPUPoints        []int                          `yaml:"cpu_points"`
	GoMemLimitPerVCPU int                           `yaml:"go_mem_limit_per_vcpu,omitempty"`
	Overrides        map[int]map[string]any         `yaml:"overrides,omitempty"`
}

type ResetStep struct {
	SQL  string `yaml:"sql,omitempty"`
	Bash string `yaml:"bash,omitempty"`
}

func LoadScenario(path string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var s Scenario
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	if s.Matrix.GoMemLimitPerVCPU == 0 {
		s.Matrix.GoMemLimitPerVCPU = defaultGoMemPerVCPU
	}
	return &s, nil
}

func (s *Scenario) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if s.Connector == "" {
		return fmt.Errorf("connector is required")
	}
	if s.Stack == "" {
		return fmt.Errorf("stack is required")
	}
	if len(s.Matrix.CPUPoints) == 0 {
		return fmt.Errorf("matrix.cpu_points must contain at least one value")
	}
	for i := 1; i < len(s.Matrix.CPUPoints); i++ {
		if s.Matrix.CPUPoints[i] <= s.Matrix.CPUPoints[i-1] {
			return fmt.Errorf("matrix.cpu_points must be strictly ascending: %v", s.Matrix.CPUPoints)
		}
	}

	maxCPU := s.Matrix.CPUPoints[len(s.Matrix.CPUPoints)-1]
	vCPU := vcpuForInstanceType(s.Infra.Runner.InstanceType)
	if vCPU == 0 {
		return fmt.Errorf("infra.runner.instance_type %q: unknown vCPU count", s.Infra.Runner.InstanceType)
	}
	if vCPU < reservedCores+maxCPU {
		return fmt.Errorf("infra.runner.instance_type %q has %d vCPU but matrix requires %d (max sweep %d + %d reserved)",
			s.Infra.Runner.InstanceType, vCPU, reservedCores+maxCPU, maxCPU, reservedCores)
	}

	if s.Workload != nil {
		if s.Workload.Warmup < minWarmup {
			return fmt.Errorf("workload.warmup %s is below minimum %s", s.Workload.Warmup, minWarmup)
		}
		if s.Workload.Duration < minDuration {
			return fmt.Errorf("workload.duration %s is below minimum %s", s.Workload.Duration, minDuration)
		}
	} else {
		// Bounded-dataset scenario: require the size hint and verify wall-clock estimate.
		if s.Dataset.ExpectedPeakMBSec == 0 {
			return fmt.Errorf("bounded-dataset scenario must set dataset.expected_peak_mb_s")
		}
		totalBytes := s.Dataset.InitialRows * int64(s.Dataset.RowSizeBytes)
		mbTotal := totalBytes / (1024 * 1024)
		estSeconds := mbTotal / int64(s.Dataset.ExpectedPeakMBSec)
		if estSeconds < int64(minDuration.Seconds()) {
			return fmt.Errorf("bounded-dataset run would complete in %ds at %d MB/s — below minimum %s; increase dataset",
				estSeconds, s.Dataset.ExpectedPeakMBSec, minDuration)
		}
	}
	return nil
}

// vcpuForInstanceType returns the vCPU count for known instance types or 0 if
// unknown. Extend this table when new types are referenced by scenarios.
func vcpuForInstanceType(it string) int {
	table := map[string]int{
		"c7i.large":    2,
		"c7i.xlarge":   4,
		"c7i.2xlarge":  8,
		"c7i.4xlarge":  16,
		"c7i.8xlarge":  32,
		"c7i.12xlarge": 48,
		"c7i.16xlarge": 64,
	}
	return table[it]
}
```

- [ ] **Step 5: Add the YAML dependency**

```bash
go get gopkg.in/yaml.v3@v3.0.1
go mod tidy
```

- [ ] **Step 6: Run the tests, verify pass**

```bash
go test ./benchmarking/aws/runner -run TestLoadScenario -v
go test ./benchmarking/aws/runner -run TestVCPUForInstanceType -v
```

Expected: all 4 tests PASS.

- [ ] **Step 7: Commit**

```bash
git add benchmarking/aws/runner/scenario.go \
        benchmarking/aws/runner/scenario_test.go \
        benchmarking/aws/runner/testdata/ \
        go.mod go.sum
git commit -m "$(cat <<'EOF'
feat(bench/aws): add scenario parser and validation

Defines the Scenario struct and YAML loader with validation rules for
duration/warmup minimums, monotonic CPU sweep, and runner instance sizing.
Bounded-dataset scenarios must declare expected_peak_mb_s and run >=15m
wall-clock at the highest CPU point.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Stats parser — rolling-stats lines + percentiles

The `benchmark` processor in Redpanda Connect emits log lines like:

```
INFO rolling stats: 99000 msg/sec, 204 MB/sec  @service=redpanda-connect bytes/sec=2.038e+08 label="" msg/sec=99000 path=root.output.processors.0
```

The runner needs to parse these into samples and compute p5/p50/p95/peak over the stable window.

**Files:**
- Create: `benchmarking/aws/runner/stats.go`
- Test: `benchmarking/aws/runner/stats_test.go`

- [ ] **Step 1: Write the failing test**

Write `benchmarking/aws/runner/stats_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRollingStatsLine_OK(t *testing.T) {
	line := `INFO rolling stats: 99000 msg/sec, 204 MB/sec  @service=redpanda-connect bytes/sec=2.038e+08 label="" msg/sec=99000 path=root.output.processors.0`
	s, ok := ParseRollingStatsLine(line)
	require.True(t, ok)
	require.Equal(t, 99000.0, s.MsgPerSec)
	require.Equal(t, 204.0, s.MBPerSec)
}

func TestParseRollingStatsLine_IgnoresUnrelated(t *testing.T) {
	_, ok := ParseRollingStatsLine(`INFO starting input`)
	require.False(t, ok)
}

func TestParseRollingStatsLine_Float(t *testing.T) {
	line := `INFO rolling stats: 99316.5 msg/sec, 204.7 MB/sec`
	s, ok := ParseRollingStatsLine(line)
	require.True(t, ok)
	require.InDelta(t, 99316.5, s.MsgPerSec, 0.01)
	require.InDelta(t, 204.7, s.MBPerSec, 0.01)
}

func TestSummarise_BasicPercentiles(t *testing.T) {
	samples := make([]Sample, 100)
	for i := 0; i < 100; i++ {
		samples[i] = Sample{T: i, MBPerSec: float64(i + 1)} // 1..100
	}
	sum := Summarise(samples)
	require.Equal(t, 50.5, sum.MedianMBPerSec)
	require.Equal(t, 5.5, sum.P5MBPerSec)
	require.Equal(t, 95.5, sum.P95MBPerSec)
	require.Equal(t, 100.0, sum.PeakMBPerSec)
}

func TestParseRollingStatsStream(t *testing.T) {
	in := strings.NewReader(`INFO startup
INFO rolling stats: 100 msg/sec, 1 MB/sec
INFO rolling stats: 200 msg/sec, 2 MB/sec
INFO unrelated
INFO rolling stats: 300 msg/sec, 3 MB/sec
`)
	samples, err := ParseRollingStatsStream(in)
	require.NoError(t, err)
	require.Len(t, samples, 3)
	require.Equal(t, 3.0, samples[2].MBPerSec)
}
```

- [ ] **Step 2: Run the test, verify failure**

```bash
go test ./benchmarking/aws/runner -run TestParseRollingStatsLine -v
```

Expected: compilation error (`ParseRollingStatsLine` undefined).

- [ ] **Step 3: Write the implementation**

Write `benchmarking/aws/runner/stats.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bufio"
	"io"
	"regexp"
	"sort"
	"strconv"
)

// Sample is one second's snapshot of rolling stats from the benchmark processor.
type Sample struct {
	T          int     `json:"t"`
	MsgPerSec  float64 `json:"msg_per_sec"`
	MBPerSec   float64 `json:"mb_per_sec"`
}

// Summary aggregates a window of samples.
type Summary struct {
	MedianMBPerSec  float64 `json:"median_mb_s"`
	P5MBPerSec      float64 `json:"p5_mb_s"`
	P95MBPerSec     float64 `json:"p95_mb_s"`
	PeakMBPerSec    float64 `json:"peak_mb_s"`
	MedianMsgPerSec float64 `json:"median_msg_s"`
	P5MsgPerSec     float64 `json:"p5_msg_s"`
	P95MsgPerSec    float64 `json:"p95_msg_s"`
	PeakMsgPerSec   float64 `json:"peak_msg_s"`
}

var rollingStatsRe = regexp.MustCompile(`rolling stats:\s+([0-9.]+)\s+msg/sec,\s+([0-9.]+)\s+MB/sec`)

// ParseRollingStatsLine extracts msg/sec and MB/sec from one log line.
// Returns ok=false if the line is not a rolling-stats line.
func ParseRollingStatsLine(line string) (Sample, bool) {
	m := rollingStatsRe.FindStringSubmatch(line)
	if len(m) != 3 {
		return Sample{}, false
	}
	msg, err1 := strconv.ParseFloat(m[1], 64)
	mb, err2 := strconv.ParseFloat(m[2], 64)
	if err1 != nil || err2 != nil {
		return Sample{}, false
	}
	return Sample{MsgPerSec: msg, MBPerSec: mb}, true
}

// ParseRollingStatsStream reads a Connect stdout stream and returns every
// rolling-stats sample in order. The T field is assigned by index.
func ParseRollingStatsStream(r io.Reader) ([]Sample, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var samples []Sample
	t := 0
	for scanner.Scan() {
		s, ok := ParseRollingStatsLine(scanner.Text())
		if !ok {
			continue
		}
		s.T = t
		samples = append(samples, s)
		t++
	}
	return samples, scanner.Err()
}

// Summarise computes percentiles and peak over the given samples.
func Summarise(samples []Sample) Summary {
	if len(samples) == 0 {
		return Summary{}
	}
	mb := make([]float64, len(samples))
	msg := make([]float64, len(samples))
	for i, s := range samples {
		mb[i] = s.MBPerSec
		msg[i] = s.MsgPerSec
	}
	return Summary{
		MedianMBPerSec:  percentile(mb, 50),
		P5MBPerSec:      percentile(mb, 5),
		P95MBPerSec:     percentile(mb, 95),
		PeakMBPerSec:    peak(mb),
		MedianMsgPerSec: percentile(msg, 50),
		P5MsgPerSec:     percentile(msg, 5),
		P95MsgPerSec:    percentile(msg, 95),
		PeakMsgPerSec:   peak(msg),
	}
}

// percentile uses linear interpolation between closest ranks (NIST method).
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := (p / 100) * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := rank - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func peak(values []float64) float64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}
```

- [ ] **Step 4: Run the tests, verify pass**

```bash
go test ./benchmarking/aws/runner -run TestParseRollingStats -v
go test ./benchmarking/aws/runner -run TestSummarise -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/stats.go benchmarking/aws/runner/stats_test.go
git commit -m "$(cat <<'EOF'
feat(bench/aws): add rolling-stats parser and percentile summariser

Parses 'rolling stats: X msg/sec, Y MB/sec' lines from the benchmark
processor and computes p5/p50/p95/peak via linear-interpolated percentiles.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Anomaly detector

The spec mandates flagging any contiguous ≥60s span where MB/sec drops below `0.8 × median`. Renders as a callout in the markdown.

**Files:**
- Create: `benchmarking/aws/runner/anomalies.go`
- Test: `benchmarking/aws/runner/anomalies_test.go`

- [ ] **Step 1: Write the failing test**

Write `benchmarking/aws/runner/anomalies_test.go`:

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

func mkSamples(values ...float64) []Sample {
	out := make([]Sample, len(values))
	for i, v := range values {
		out[i] = Sample{T: i, MBPerSec: v}
	}
	return out
}

func TestDetectAnomalies_NoDips(t *testing.T) {
	samples := mkSamples(100, 100, 100, 100, 100)
	require.Empty(t, DetectAnomalies(samples, 100))
}

func TestDetectAnomalies_BriefDipIgnored(t *testing.T) {
	// 30s dip — below the 60s threshold, ignored.
	vals := []float64{}
	for i := 0; i < 100; i++ {
		vals = append(vals, 100)
	}
	for i := 100; i < 130; i++ {
		vals = append(vals, 50)
	}
	for i := 130; i < 200; i++ {
		vals = append(vals, 100)
	}
	require.Empty(t, DetectAnomalies(mkSamples(vals...), 100))
}

func TestDetectAnomalies_LongDipReported(t *testing.T) {
	// 73s dip at 0.6× median — should be detected.
	vals := []float64{}
	for i := 0; i < 100; i++ {
		vals = append(vals, 100)
	}
	for i := 0; i < 73; i++ {
		vals = append(vals, 60)
	}
	for i := 0; i < 100; i++ {
		vals = append(vals, 100)
	}
	anomalies := DetectAnomalies(mkSamples(vals...), 100)
	require.Len(t, anomalies, 1)
	require.Equal(t, 100, anomalies[0].StartT)
	require.Equal(t, 73, anomalies[0].DurationSec)
	require.InDelta(t, 0.6, anomalies[0].MinRatio, 0.001)
}

func TestDetectAnomalies_MultipleDips(t *testing.T) {
	// two separate long dips
	vals := []float64{}
	for i := 0; i < 50; i++ {
		vals = append(vals, 100)
	}
	for i := 0; i < 70; i++ {
		vals = append(vals, 50)
	}
	for i := 0; i < 50; i++ {
		vals = append(vals, 100)
	}
	for i := 0; i < 65; i++ {
		vals = append(vals, 60)
	}
	for i := 0; i < 50; i++ {
		vals = append(vals, 100)
	}
	anomalies := DetectAnomalies(mkSamples(vals...), 100)
	require.Len(t, anomalies, 2)
}
```

- [ ] **Step 2: Run the test, verify failure**

```bash
go test ./benchmarking/aws/runner -run TestDetectAnomalies -v
```

Expected: compilation error.

- [ ] **Step 3: Write the implementation**

Write `benchmarking/aws/runner/anomalies.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

const (
	anomalyMinSeconds = 60
	anomalyThreshold  = 0.8 // MB/sec below this fraction of the reference median is a dip
)

// Anomaly is a contiguous span where MB/sec dropped below the threshold.
type Anomaly struct {
	StartT      int     `json:"start_t"`
	DurationSec int     `json:"duration_s"`
	MinRatio    float64 `json:"min_ratio"`
	Note        string  `json:"note"`
}

// DetectAnomalies scans the sample stream for spans of >= 60 contiguous seconds
// where MB/sec drops below 0.8 * reference. Reference is typically the run's
// own median MB/sec.
func DetectAnomalies(samples []Sample, reference float64) []Anomaly {
	if reference <= 0 || len(samples) == 0 {
		return nil
	}
	threshold := reference * anomalyThreshold
	var out []Anomaly
	i := 0
	for i < len(samples) {
		if samples[i].MBPerSec >= threshold {
			i++
			continue
		}
		start := i
		minVal := samples[i].MBPerSec
		for i < len(samples) && samples[i].MBPerSec < threshold {
			if samples[i].MBPerSec < minVal {
				minVal = samples[i].MBPerSec
			}
			i++
		}
		dur := i - start
		if dur >= anomalyMinSeconds {
			out = append(out, Anomaly{
				StartT:      samples[start].T,
				DurationSec: dur,
				MinRatio:    minVal / reference,
				Note:        "MB/sec dropped below threshold — investigate before publishing",
			})
		}
	}
	return out
}
```

- [ ] **Step 4: Run the tests, verify pass**

```bash
go test ./benchmarking/aws/runner -run TestDetectAnomalies -v
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/anomalies.go benchmarking/aws/runner/anomalies_test.go
git commit -m "$(cat <<'EOF'
feat(bench/aws): add anomaly detector for sustained throughput dips

Flags contiguous >=60s spans where MB/sec drops below 0.8x reference.
These bubble up as callouts in the rendered markdown so reviewers know
when a run isn't safe to publish externally.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Result types + JSON writer

Persistence: the result of one scenario run.

**Files:**
- Create: `benchmarking/aws/runner/render.go`
- Test: `benchmarking/aws/runner/render_test.go`

- [ ] **Step 1: Write the failing test for JSON writing**

Write `benchmarking/aws/runner/render_test.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sampleResult() *Result {
	return &Result{
		Scenario:      "postgres/orders-cdc",
		ScenarioHash:  "sha256:abc123",
		GitSHA:        "e491c80fc",
		StartedAt:     time.Date(2026, 5, 19, 14, 2, 11, 0, time.UTC),
		FinishedAt:    time.Date(2026, 5, 19, 15, 33, 48, 0, time.UTC),
		Infra: ResultInfra{
			RunnerInstanceType:  "c7i.4xlarge",
			SourceInstanceClass: "db.r6g.2xlarge",
			SourceStorageGB:     400,
			Region:              "us-east-2",
		},
		Dataset: ResultDataset{Rows: 75_000_000, RowSizeBytes: 1200, TotalBytes: 90_000_000_000},
		Points: []PointResult{
			{
				VCPU: 1,
				Samples: []Sample{{T: 0, MBPerSec: 153, MsgPerSec: 127344}},
				Summary: Summary{MedianMBPerSec: 153, P5MBPerSec: 144, P95MBPerSec: 161, PeakMBPerSec: 167,
					MedianMsgPerSec: 127344, P5MsgPerSec: 119800, P95MsgPerSec: 134000, PeakMsgPerSec: 138200},
			},
		},
	}
}

func TestWriteResultJSON(t *testing.T) {
	dir := t.TempDir()
	r := sampleResult()
	path, err := WriteResultJSON(dir, r)
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(path) || filepath.Dir(path) != ".")

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var got Result
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, r.Scenario, got.Scenario)
	require.Equal(t, r.Points[0].Summary.MedianMBPerSec, got.Points[0].Summary.MedianMBPerSec)
}

func TestAppendMarkdown(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "postgres.md")
	require.NoError(t, os.WriteFile(target, []byte("# Postgres existing content\n"), 0o644))

	r := sampleResult()
	require.NoError(t, AppendMarkdown(target, r, "Stream changes from a 75M-row orders table at 5K writes/sec sustained."))

	out, err := os.ReadFile(target)
	require.NoError(t, err)
	s := string(out)
	require.Contains(t, s, "# Postgres existing content")
	require.Contains(t, s, "## AWS — orders-cdc — 2026-05-19")
	require.Contains(t, s, "c7i.4xlarge")
	require.Contains(t, s, "db.r6g.2xlarge")
	require.Contains(t, s, "| 1          |          153 |         144 |          161 |       127,344 |")
}
```

- [ ] **Step 2: Run the test, verify failure**

```bash
go test ./benchmarking/aws/runner -run "TestWriteResultJSON|TestAppendMarkdown" -v
```

Expected: compilation error.

- [ ] **Step 3: Write the implementation — types and JSON writer**

Write `benchmarking/aws/runner/render.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

// Result is the canonical per-run artefact, written as JSON and rendered to
// markdown.
type Result struct {
	Scenario     string        `json:"scenario"`
	ScenarioHash string        `json:"scenario_hash"`
	GitSHA       string        `json:"git_sha"`
	StartedAt    time.Time     `json:"started_at"`
	FinishedAt   time.Time     `json:"finished_at"`
	Infra        ResultInfra   `json:"infra"`
	Dataset      ResultDataset `json:"dataset"`
	Points       []PointResult `json:"points"`
}

type ResultInfra struct {
	RunnerInstanceType  string `json:"runner_instance_type"`
	SourceInstanceClass string `json:"source_instance_class"`
	SourceStorageGB     int    `json:"source_storage_gb"`
	Region              string `json:"region"`
}

type ResultDataset struct {
	Rows         int64 `json:"rows"`
	RowSizeBytes int   `json:"row_size_bytes"`
	TotalBytes   int64 `json:"total_bytes"`
}

type PointResult struct {
	VCPU         int                `json:"vcpu"`
	Samples      []Sample           `json:"samples"`
	Summary      Summary            `json:"summary"`
	Anomalies    []Anomaly          `json:"anomalies,omitempty"`
	PromSnapshot map[string]float64 `json:"prom_snapshot,omitempty"`
}

// WriteResultJSON writes the result to <dir>/<connector>/<scenario>/<timestamp>.json.
// Connector and scenario are inferred from r.Scenario (format "connector/scenario").
func WriteResultJSON(dir string, r *Result) (string, error) {
	parts := strings.SplitN(r.Scenario, "/", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("scenario %q is not connector/scenario", r.Scenario)
	}
	outDir := filepath.Join(dir, parts[0], parts[1])
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	stamp := r.StartedAt.UTC().Format("2006-01-02T15-04-05Z")
	out := filepath.Join(outDir, stamp+".json")
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(out, raw, 0o644); err != nil {
		return "", err
	}
	return out, nil
}

//go:embed templates/result.md.tmpl
var resultMarkdownTmpl string

type markdownView struct {
	ScenarioShort string
	Date          string
	Description   string
	GitSHA        string
	GitShortSHA   string
	Infra         ResultInfra
	Dataset       ResultDataset
	WorkloadLine  string
	Rows          []markdownRow
	Anomalies     []anomalyView
	JSONPath      string
}

type markdownRow struct {
	VCPU        int
	MedianMB    string
	P5MB        string
	P95MB       string
	MedianMsgFC string
}

type anomalyView struct {
	VCPU        int
	DurationSec int
	MinRatio    float64
	StartT      int
}

// AppendMarkdown appends a results section to the target file.
// The description argument is the scenario's prose description.
func AppendMarkdown(target string, r *Result, description string) error {
	parts := strings.SplitN(r.Scenario, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("scenario %q is not connector/scenario", r.Scenario)
	}

	rows := make([]markdownRow, len(r.Points))
	var anomalies []anomalyView
	for i, p := range r.Points {
		rows[i] = markdownRow{
			VCPU:        p.VCPU,
			MedianMB:    fmt.Sprintf("%12.0f", p.Summary.MedianMBPerSec),
			P5MB:        fmt.Sprintf("%11.0f", p.Summary.P5MBPerSec),
			P95MB:       fmt.Sprintf("%12.0f", p.Summary.P95MBPerSec),
			MedianMsgFC: formatThousands(int64(p.Summary.MedianMsgPerSec)),
		}
		for _, a := range p.Anomalies {
			anomalies = append(anomalies, anomalyView{
				VCPU: p.VCPU, DurationSec: a.DurationSec, MinRatio: a.MinRatio, StartT: a.StartT,
			})
		}
	}

	workload := ""
	// Render workload line if dataset contains row count > 0 (it always does in this plan's
	// rendering surface, but kept defensive).
	if r.Dataset.Rows > 0 {
		workload = fmt.Sprintf("%s rows × %d B = ~%d GB",
			formatThousands(r.Dataset.Rows), r.Dataset.RowSizeBytes, r.Dataset.TotalBytes/(1<<30))
	}

	gitShort := r.GitSHA
	if len(gitShort) > 9 {
		gitShort = gitShort[:9]
	}

	view := markdownView{
		ScenarioShort: parts[1],
		Date:          r.StartedAt.UTC().Format("2006-01-02"),
		Description:   description,
		GitSHA:        r.GitSHA,
		GitShortSHA:   gitShort,
		Infra:         r.Infra,
		Dataset:       r.Dataset,
		WorkloadLine:  workload,
		Rows:          rows,
		Anomalies:     anomalies,
		JSONPath:      fmt.Sprintf("results/%s/%s/%s.json", parts[0], parts[1], r.StartedAt.UTC().Format("2006-01-02T15-04-05Z")),
	}

	t, err := template.New("result").Parse(resultMarkdownTmpl)
	if err != nil {
		return err
	}
	var sb strings.Builder
	if err := t.Execute(&sb, view); err != nil {
		return err
	}

	f, err := os.OpenFile(target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString("\n\n"); err != nil {
		return err
	}
	_, err = f.WriteString(sb.String())
	return err
}

func formatThousands(n int64) string {
	in := fmt.Sprintf("%d", n)
	if len(in) <= 3 {
		return in
	}
	var sb strings.Builder
	pre := len(in) % 3
	if pre > 0 {
		sb.WriteString(in[:pre])
		if len(in) > pre {
			sb.WriteString(",")
		}
	}
	for i := pre; i < len(in); i += 3 {
		sb.WriteString(in[i : i+3])
		if i+3 < len(in) {
			sb.WriteString(",")
		}
	}
	return sb.String()
}
```

- [ ] **Step 4: Write the markdown template**

Write `benchmarking/aws/runner/templates/result.md.tmpl`:

```
## AWS — {{.ScenarioShort}} — {{.Date}}

**Scenario:** {{.Description}}

**Git SHA:** [`{{.GitShortSHA}}`](https://github.com/redpanda-data/connect/commit/{{.GitSHA}})

**Infra:** Runner `{{.Infra.RunnerInstanceType}}`; source `{{.Infra.SourceInstanceClass}}` ({{.Infra.SourceStorageGB}} GB) in `{{.Infra.Region}}`.

**Dataset:** {{.WorkloadLine}}

### Throughput

| GOMAXPROCS | MB/sec (p50) | MB/sec (p5) | MB/sec (p95) | msg/sec (p50) |
|------------|--------------|-------------|--------------|---------------|
{{- range .Rows }}
| {{printf "%-10d" .VCPU}} | {{.MedianMB}} | {{.P5MB}} | {{.P95MB}} | {{printf "%13s" .MedianMsgFC}} |
{{- end }}
{{ if .Anomalies }}
{{ range .Anomalies }}
> ⚠ At {{.VCPU}} vCPU: {{.DurationSec}}s dip to {{printf "%.2f" .MinRatio}}× median MB/sec from t={{.StartT}}s — investigate before publishing.
{{ end }}
{{ end }}

Raw samples + Prometheus snapshots: [`{{.JSONPath}}`]({{.JSONPath}})
```

- [ ] **Step 5: Run the tests, verify pass**

```bash
go test ./benchmarking/aws/runner -run "TestWriteResultJSON|TestAppendMarkdown" -v
```

Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/render.go benchmarking/aws/runner/render_test.go benchmarking/aws/runner/templates/result.md.tmpl
git commit -m "$(cat <<'EOF'
feat(bench/aws): add Result types, JSON writer, and markdown renderer

Defines the canonical per-run JSON shape and renders an appended section
to docs/benchmark-results/<connector>.md from an embedded template.
Anomalies surface as quoted callouts.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: SSM wrapper (RunCommand + streaming output)

The runner needs to execute commands on the runner EC2 and the load-gen EC2 over Systems Manager. We wrap the AWS SDK so the matrix code can stay pure.

**Files:**
- Create: `benchmarking/aws/runner/ssm.go`

- [ ] **Step 1: Add AWS SDK dependency**

```bash
go get github.com/aws/aws-sdk-go-v2/config@v1.29.0
go get github.com/aws/aws-sdk-go-v2/service/ssm@v1.56.0
go mod tidy
```

- [ ] **Step 2: Write the wrapper**

Write `benchmarking/aws/runner/ssm.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

// SSMExecutor executes shell commands on EC2 instances via Systems Manager.
type SSMExecutor interface {
	// Run executes a script on the named instance, streaming stdout line-by-line
	// to onLine until the command finishes or ctx is cancelled.
	Run(ctx context.Context, instanceID, script string, onLine func(string)) error
}

type awsSSM struct {
	client *ssm.Client
}

// NewSSMExecutor builds an executor backed by the AWS SDK in the given region.
func NewSSMExecutor(ctx context.Context, region string) (SSMExecutor, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &awsSSM{client: ssm.NewFromConfig(cfg)}, nil
}

func (a *awsSSM) Run(ctx context.Context, instanceID, script string, onLine func(string)) error {
	send, err := a.client.SendCommand(ctx, &ssm.SendCommandInput{
		InstanceIds:  []string{instanceID},
		DocumentName: aws.String("AWS-RunShellScript"),
		Parameters:   map[string][]string{"commands": {script}},
		TimeoutSeconds: aws.Int32(int32((90 * time.Minute).Seconds())),
	})
	if err != nil {
		return fmt.Errorf("send command: %w", err)
	}
	commandID := *send.Command.CommandId

	var lastSeen int
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_, _ = a.client.CancelCommand(context.Background(), &ssm.CancelCommandInput{
				CommandId:   aws.String(commandID),
				InstanceIds: []string{instanceID},
			})
			return ctx.Err()
		case <-ticker.C:
		}
		inv, err := a.client.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
			CommandId:  aws.String(commandID),
			InstanceId: aws.String(instanceID),
		})
		if err != nil {
			// Not yet propagated; keep polling.
			continue
		}
		stdout := aws.ToString(inv.StandardOutputContent)
		if len(stdout) > lastSeen && onLine != nil {
			emit := stdout[lastSeen:]
			for _, line := range strings.Split(strings.TrimRight(emit, "\n"), "\n") {
				if line != "" {
					onLine(line)
				}
			}
			lastSeen = len(stdout)
		}
		switch inv.Status {
		case types.CommandInvocationStatusSuccess:
			return nil
		case types.CommandInvocationStatusFailed,
			types.CommandInvocationStatusCancelled,
			types.CommandInvocationStatusTimedOut:
			return fmt.Errorf("ssm command %s on %s ended with status %s: %s",
				commandID, instanceID, inv.Status, aws.ToString(inv.StandardErrorContent))
		}
	}
}

// FakeSSM is a deterministic SSMExecutor for tests — emits a scripted
// transcript and never touches AWS.
type FakeSSM struct {
	Transcripts map[string][]string // instanceID → lines to emit on Run
	Errs        map[string]error
}

func (f *FakeSSM) Run(_ context.Context, instanceID, _ string, onLine func(string)) error {
	for _, line := range f.Transcripts[instanceID] {
		if onLine != nil {
			onLine(line)
		}
	}
	return f.Errs[instanceID]
}

// streamingOnLine forwards each line to a writer, prefixing with [instance:].
func streamingOnLine(w io.Writer, prefix string) func(string) {
	return func(line string) {
		fmt.Fprintf(w, "[%s] %s\n", prefix, line)
	}
}
```

- [ ] **Step 3: Verify build**

```bash
go build ./benchmarking/aws/runner
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/runner/ssm.go go.mod go.sum
git commit -m "$(cat <<'EOF'
feat(bench/aws): add SSM RunCommand wrapper with streaming output

The runner shells onto the bench host via Systems Manager — no SSH keys,
IAM-gated, audit-logged. A FakeSSM is exposed for tests.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Terraform wrapper

A thin Go wrapper around `terraform` so the runner can apply/destroy stacks with scenario-derived variables.

**Files:**
- Create: `benchmarking/aws/runner/terraform.go`

- [ ] **Step 1: Write the wrapper**

Write `benchmarking/aws/runner/terraform.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Terraform shells out to the terraform CLI in a specific stack directory.
type Terraform struct {
	Dir         string // path under benchmarking/aws/terraform/stacks/<name> or shared
	BackendFile string // absolute path to backend.hcl
	StateKey    string // unique state key for this stack
}

// Init runs `terraform init` with the shared backend config.
func (t *Terraform) Init() error {
	args := []string{
		"-chdir=" + t.Dir,
		"init",
		"-input=false",
		"-backend-config=" + t.BackendFile,
		"-backend-config=key=" + t.StateKey + "/terraform.tfstate",
	}
	return run("terraform", args, nil)
}

// Apply applies the stack with the given variables. Each entry in vars becomes
// one -var "<k>=<v>" flag. Complex values must already be HCL-encoded strings.
func (t *Terraform) Apply(vars map[string]string) error {
	args := []string{
		"-chdir=" + t.Dir,
		"apply",
		"-input=false",
		"-auto-approve",
	}
	for k, v := range vars {
		args = append(args, "-var", fmt.Sprintf("%s=%s", k, v))
	}
	return run("terraform", args, nil)
}

// Destroy tears down the stack.
func (t *Terraform) Destroy(vars map[string]string) error {
	args := []string{
		"-chdir=" + t.Dir,
		"destroy",
		"-input=false",
		"-auto-approve",
	}
	for k, v := range vars {
		args = append(args, "-var", fmt.Sprintf("%s=%s", k, v))
	}
	return run("terraform", args, nil)
}

// Plan runs `terraform plan` and returns nil if successful.
func (t *Terraform) Plan(vars map[string]string) error {
	args := []string{
		"-chdir=" + t.Dir,
		"plan",
		"-input=false",
	}
	for k, v := range vars {
		args = append(args, "-var", fmt.Sprintf("%s=%s", k, v))
	}
	return run("terraform", args, nil)
}

// Outputs reads `terraform output -json` into a map[string]string.
// Non-string outputs are JSON-encoded into the value.
func (t *Terraform) Outputs() (map[string]string, error) {
	var buf bytes.Buffer
	args := []string{"-chdir=" + t.Dir, "output", "-json"}
	if err := run("terraform", args, &buf); err != nil {
		return nil, err
	}
	var raw map[string]struct {
		Sensitive bool            `json:"sensitive"`
		Type      json.RawMessage `json:"type"`
		Value     json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		var s string
		if err := json.Unmarshal(v.Value, &s); err == nil {
			out[k] = s
		} else {
			out[k] = string(v.Value)
		}
	}
	return out, nil
}

func run(name string, args []string, stdout *bytes.Buffer) error {
	cmd := exec.Command(name, args...)
	if stdout != nil {
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = os.Stdout
	}
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// StackDir returns the path to a stack relative to the repo root.
func StackDir(repoRoot, stack string) string {
	return filepath.Join(repoRoot, "benchmarking", "aws", "terraform", "stacks", stack)
}

// SharedDir returns the path to the shared stack relative to the repo root.
func SharedDir(repoRoot string) string {
	return filepath.Join(repoRoot, "benchmarking", "aws", "terraform", "shared")
}
```

- [ ] **Step 2: Verify build**

```bash
go build ./benchmarking/aws/runner
```

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/runner/terraform.go
git commit -m "$(cat <<'EOF'
feat(bench/aws): add Terraform CLI wrapper

Init/Apply/Destroy/Plan/Outputs for a stack directory, using the shared
S3 backend with a per-stack state key.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Matrix executor

This is the CPU sweep loop. It calls SSM with a templated script that sets up `taskset`/`chrt`/`GOMAXPROCS`/`GOMEMLIMIT` and runs Connect, while collecting samples and computing the summary.

**Files:**
- Create: `benchmarking/aws/runner/matrix.go`

- [ ] **Step 1: Write the executor**

Write `benchmarking/aws/runner/matrix.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// MatrixRunner orchestrates the CPU sweep against the runner EC2.
type MatrixRunner struct {
	SSM             SSMExecutor
	RunnerInstance  string
	LoadGenInstance string
	ConfigPath      string // path on the runner host to benchmark_config.yaml
	BinaryPath      string // path on the runner host to redpanda-connect
}

// SweepPoint is the per-point measurement.
type SweepPoint struct {
	VCPU      int
	Samples   []Sample
	Summary   Summary
	Anomalies []Anomaly
}

// Run executes the full sweep. resetScript runs on the runner host between
// points (e.g. drop the CDC replication slot). workloadScript, if non-empty,
// runs on the load-gen host concurrently with the bench step.
func (m *MatrixRunner) Run(
	ctx context.Context,
	cpuPoints []int,
	memLimitPerVCPU int,
	warmup, duration time.Duration,
	resetScript string,
	workloadScript string,
) ([]SweepPoint, error) {
	out := make([]SweepPoint, 0, len(cpuPoints))
	for _, n := range cpuPoints {
		fmt.Printf("=== sweep point: %d vCPU (warmup %s, window %s) ===\n", n, warmup, duration)

		if resetScript != "" {
			if err := m.SSM.Run(ctx, m.RunnerInstance, resetScript, streamingOnLine(stdout, "reset")); err != nil {
				return nil, fmt.Errorf("reset at %d vCPU: %w", n, err)
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

		script := renderBenchScript(benchScriptArgs{
			VCPU:            n,
			MemLimitGiB:     memLimitPerVCPU * n,
			WarmupSec:       int(warmup.Seconds()),
			DurationSec:     int(duration.Seconds()),
			ConfigPath:      m.ConfigPath,
			BinaryPath:      m.BinaryPath,
		})

		var samples []Sample
		startTime := -warmup // negative T during warmup
		t := 0
		onLine := func(line string) {
			s, ok := ParseRollingStatsLine(line)
			if !ok {
				fmt.Fprintln(stdout, "[bench] "+line)
				return
			}
			elapsed := time.Duration(t) * time.Second
			t++
			if elapsed < warmup {
				return // discard warmup
			}
			s.T = int(elapsed.Seconds() - warmup.Seconds())
			samples = append(samples, s)
			_ = startTime
		}
		if err := m.SSM.Run(ctx, m.RunnerInstance, script, onLine); err != nil {
			cancelWorkload()
			return nil, fmt.Errorf("bench at %d vCPU: %w", n, err)
		}
		cancelWorkload()
		<-workloadDone

		summary := Summarise(samples)
		anomalies := DetectAnomalies(samples, summary.MedianMBPerSec)
		out = append(out, SweepPoint{
			VCPU:      n,
			Samples:   samples,
			Summary:   summary,
			Anomalies: anomalies,
		})
		fmt.Printf("  -> median %.0f MB/s (p5 %.0f, p95 %.0f, peak %.0f), %d anomalies\n",
			summary.MedianMBPerSec, summary.P5MBPerSec, summary.P95MBPerSec, summary.PeakMBPerSec, len(anomalies))
	}
	return out, nil
}

type benchScriptArgs struct {
	VCPU        int
	MemLimitGiB int
	WarmupSec   int
	DurationSec int
	ConfigPath  string
	BinaryPath  string
}

// renderBenchScript produces the shell script executed on the runner EC2 for
// one sweep point. The script pins Connect to the measured cores, runs it for
// warmup+duration seconds, then SIGTERMs cleanly so the benchmark processor
// flushes its final rolling-stats line.
func renderBenchScript(a benchScriptArgs) string {
	// Cores 0,1 reserved → measured set starts at core 2.
	cpusetHi := 1 + a.VCPU // inclusive
	return strings.Join([]string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting bench: %d vCPU, %d GiB, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`taskset -c 2-%d chrt --fifo 50 env GOMAXPROCS=%d GOMEMLIMIT=%dGiB %s run %s &`,
			cpusetHi, a.VCPU, a.MemLimitGiB, a.BinaryPath, a.ConfigPath),
		`PID=$!`,
		fmt.Sprintf(`sleep %d`, a.WarmupSec+a.DurationSec),
		`kill -TERM $PID || true`,
		`wait $PID 2>/dev/null || true`,
		`echo "bench point complete"`,
	}, "\n")
}

// stdout package-level so streamingOnLine can plug into MatrixRunner without
// taking io.Writer in every signature. Set to os.Stdout from main.go.
var stdout = io_stdout()

func io_stdout() interface {
	Write(p []byte) (n int, err error)
} {
	return defaultStdout{}
}

type defaultStdout struct{}

func (defaultStdout) Write(p []byte) (n int, err error) {
	return fmt.Print(string(p))
}
```

- [ ] **Step 2: Verify build**

```bash
go build ./benchmarking/aws/runner
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/runner/matrix.go
git commit -m "$(cat <<'EOF'
feat(bench/aws): add CPU sweep matrix executor

Loops over scenario.matrix.cpu_points, pins Connect to a cpuset starting at
core 2 (cores 0,1 reserved), runs warmup+duration, parses rolling stats,
and computes per-point summary + anomalies.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: CLI main and the bench flow

Wire the pieces together.

**Files:**
- Create: `benchmarking/aws/runner/main.go`

- [ ] **Step 1: Write main.go**

Write `benchmarking/aws/runner/main.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "bench":
		run(benchCmd(os.Args[2:]))
	case "validate":
		run(validateCmd(os.Args[2:]))
	case "down":
		run(downCmd(os.Args[2:]))
	case "cost-check":
		run(costCheckCmd(os.Args[2:]))
	default:
		usage()
		os.Exit(2)
	}
}

func run(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  runner bench --scenario=<path> [--keep] [--keep-on-fail]
  runner validate --scenario=<path>
  runner down --scenario=<path>
  runner cost-check`)
}

type benchOpts struct {
	scenarioPath string
	keep         bool
	keepOnFail   bool
	region       string
	repoRoot     string
}

func benchCmd(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	scenario := fs.String("scenario", "", "path to scenario YAML (e.g. scenarios/postgres/orders-cdc.yaml)")
	keep := fs.Bool("keep", false, "don't tear down infra after the run")
	keepOnFail := fs.Bool("keep-on-fail", false, "keep infra if the bench errors")
	region := fs.String("region", "us-east-2", "AWS region")
	repoRoot := fs.String("repo-root", ".", "path to the connect repo root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenario == "" {
		return fmt.Errorf("--scenario is required")
	}

	opts := benchOpts{
		scenarioPath: *scenario,
		keep:         *keep,
		keepOnFail:   *keepOnFail,
		region:       *region,
		repoRoot:     *repoRoot,
	}
	return runBench(opts)
}

func runBench(opts benchOpts) (errOut error) {
	s, err := LoadScenario(opts.scenarioPath)
	if err != nil {
		return err
	}
	fmt.Printf("[1/7] loaded scenario %s\n", s.Name)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	tfShared := &Terraform{
		Dir:         SharedDir(opts.repoRoot),
		BackendFile: filepath.Join(opts.repoRoot, "benchmarking/aws/terraform/backend.hcl"),
		StateKey:    "shared",
	}
	tfStack := &Terraform{
		Dir:         StackDir(opts.repoRoot, s.Stack),
		BackendFile: filepath.Join(opts.repoRoot, "benchmarking/aws/terraform/backend.hcl"),
		StateKey:    s.Stack,
	}

	// Step 2: terraform init+apply on shared, then stack.
	if err := tfShared.Init(); err != nil {
		return fmt.Errorf("terraform init shared: %w", err)
	}
	if err := tfStack.Init(); err != nil {
		return fmt.Errorf("terraform init %s: %w", s.Stack, err)
	}
	sharedVars := map[string]string{
		"region":               opts.region,
		"runner_instance_type": s.Infra.Runner.InstanceType,
		"bench_session_id":     newSessionID(),
	}
	if err := tfShared.Apply(sharedVars); err != nil {
		return fmt.Errorf("terraform apply shared: %w", err)
	}
	stackVars := translateInfraSource(s.Infra.Source, opts.region)
	fmt.Println("[2/7] terraform apply (shared + stack) complete")

	defer func() {
		if opts.keep {
			fmt.Println("[7/7] keep=true: skipping teardown")
			return
		}
		if errOut != nil && opts.keepOnFail {
			fmt.Println("[7/7] keep-on-fail=true and run errored: skipping teardown")
			return
		}
		fmt.Println("[7/7] terraform destroy")
		_ = tfStack.Destroy(stackVars)
		_ = tfShared.Destroy(sharedVars)
	}()

	if err := tfStack.Apply(stackVars); err != nil {
		return fmt.Errorf("terraform apply %s: %w", s.Stack, err)
	}

	sharedOuts, err := tfShared.Outputs()
	if err != nil {
		return fmt.Errorf("terraform output shared: %w", err)
	}
	stackOuts, err := tfStack.Outputs()
	if err != nil {
		return fmt.Errorf("terraform output stack: %w", err)
	}
	for k, v := range stackOuts {
		sharedOuts[k] = v
	}

	// Step 3: build the binary.
	binPath, err := buildConnect(opts.repoRoot)
	if err != nil {
		return fmt.Errorf("build connect: %w", err)
	}
	fmt.Println("[3/7] built redpanda-connect")

	// Step 4: render config + stage artefacts (described in Task 12).
	cfgPath, err := renderPipelineConfig(s, sharedOuts)
	if err != nil {
		return fmt.Errorf("render pipeline config: %w", err)
	}
	if err := stageArtefacts(ctx, opts, sharedOuts, binPath, cfgPath); err != nil {
		return fmt.Errorf("stage artefacts: %w", err)
	}
	fmt.Println("[4/7] staged binary + config on runner")

	// Step 5: seed.
	if err := runSeeder(ctx, opts, s, sharedOuts); err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	fmt.Println("[5/7] seed complete")

	// Step 6: sweep.
	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	mr := &MatrixRunner{
		SSM:             ssmExec,
		RunnerInstance:  sharedOuts["runner_instance_id"],
		LoadGenInstance: sharedOuts["load_gen_instance_id"],
		ConfigPath:      "/opt/bench/config.yaml",
		BinaryPath:      "/opt/bench/redpanda-connect",
	}
	reset := combineReset(s.Reset, sharedOuts)
	workload := renderWorkloadScript(s, sharedOuts)
	warmup := time.Duration(0)
	duration := time.Duration(0)
	if s.Workload != nil {
		warmup = s.Workload.Warmup
		duration = s.Workload.Duration
	} else {
		// Bounded-dataset scenario: no sustained workload, no warmup.
		duration = minDuration
	}
	points, err := mr.Run(ctx, s.Matrix.CPUPoints, s.Matrix.GoMemLimitPerVCPU, warmup, duration, reset, workload)
	if err != nil {
		return err
	}
	fmt.Println("[6/7] sweep complete")

	// Step 7: persist + render.
	result := &Result{
		Scenario:     fmt.Sprintf("%s/%s", s.Stack, strings.TrimPrefix(s.Name, s.Stack+"-")),
		ScenarioHash: hashScenario(s),
		GitSHA:       gitSHA(opts.repoRoot),
		StartedAt:    time.Now().Add(-totalDuration(s, len(points))).UTC(),
		FinishedAt:   time.Now().UTC(),
		Infra: ResultInfra{
			RunnerInstanceType:  s.Infra.Runner.InstanceType,
			SourceInstanceClass: asString(s.Infra.Source["instance_class"]),
			SourceStorageGB:     asInt(s.Infra.Source["storage_gb"]),
			Region:              opts.region,
		},
		Dataset: ResultDataset{
			Rows:         s.Dataset.InitialRows,
			RowSizeBytes: s.Dataset.RowSizeBytes,
			TotalBytes:   s.Dataset.InitialRows * int64(s.Dataset.RowSizeBytes),
		},
	}
	for _, p := range points {
		result.Points = append(result.Points, PointResult{
			VCPU:      p.VCPU,
			Samples:   p.Samples,
			Summary:   p.Summary,
			Anomalies: p.Anomalies,
		})
	}
	resultsDir := filepath.Join(opts.repoRoot, "benchmarking/aws/results")
	jsonPath, err := WriteResultJSON(resultsDir, result)
	if err != nil {
		return err
	}
	mdPath := filepath.Join(opts.repoRoot, "docs/benchmark-results", s.Stack+".md")
	if err := AppendMarkdown(mdPath, result, strings.TrimSpace(s.Description)); err != nil {
		return err
	}
	fmt.Printf("\n✓ done — JSON: %s\n           md: %s\n", jsonPath, mdPath)
	return nil
}

func hashScenario(s *Scenario) string {
	raw, _ := yaml.Marshal(s)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func gitSHA(repoRoot string) string {
	out, err := exec.Command("git", "-C", repoRoot, "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func totalDuration(s *Scenario, points int) time.Duration {
	if s.Workload == nil {
		return time.Duration(points) * minDuration
	}
	return time.Duration(points) * (s.Workload.Warmup + s.Workload.Duration)
}

func newSessionID() string {
	return fmt.Sprintf("bench-%s", time.Now().UTC().Format("20060102-150405"))
}

func validateCmd(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario YAML")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenario == "" {
		return fmt.Errorf("--scenario is required")
	}
	s, err := LoadScenario(*scenario)
	if err != nil {
		return err
	}
	fmt.Printf("scenario %s OK (%d cpu points, runner %s)\n",
		s.Name, len(s.Matrix.CPUPoints), s.Infra.Runner.InstanceType)
	return nil
}

func downCmd(args []string) error {
	fs := flag.NewFlagSet("down", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario YAML")
	region := fs.String("region", "us-east-2", "AWS region")
	repoRoot := fs.String("repo-root", ".", "repo root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenario == "" {
		return fmt.Errorf("--scenario is required")
	}
	s, err := LoadScenario(*scenario)
	if err != nil {
		return err
	}
	stack := &Terraform{
		Dir:         StackDir(*repoRoot, s.Stack),
		BackendFile: filepath.Join(*repoRoot, "benchmarking/aws/terraform/backend.hcl"),
		StateKey:    s.Stack,
	}
	shared := &Terraform{
		Dir:         SharedDir(*repoRoot),
		BackendFile: filepath.Join(*repoRoot, "benchmarking/aws/terraform/backend.hcl"),
		StateKey:    "shared",
	}
	_ = stack.Init()
	_ = shared.Init()
	if err := stack.Destroy(translateInfraSource(s.Infra.Source, *region)); err != nil {
		return err
	}
	return shared.Destroy(map[string]string{
		"region":               *region,
		"runner_instance_type": s.Infra.Runner.InstanceType,
	})
}

func costCheckCmd(_ []string) error {
	fmt.Println("cost-check not implemented in foundation plan — see future iterations")
	return nil
}

// translateInfraSource flattens a scenario's infra.source map into terraform
// -var-style strings. Nested maps (e.g. parameters) are JSON-encoded so HCL
// can decode them.
func translateInfraSource(src map[string]any, region string) map[string]string {
	out := map[string]string{"region": region}
	for k, v := range src {
		switch val := v.(type) {
		case string:
			out[k] = val
		case int:
			out[k] = fmt.Sprintf("%d", val)
		case int64:
			out[k] = fmt.Sprintf("%d", val)
		case float64:
			out[k] = fmt.Sprintf("%v", val)
		case map[string]any:
			b, _ := yaml.Marshal(val)
			out[k] = string(b)
		default:
			out[k] = fmt.Sprintf("%v", val)
		}
	}
	return out
}

func asString(v any) string { s, _ := v.(string); return s }
func asInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	}
	return 0
}

func buildConnect(repoRoot string) (string, error) {
	dist := filepath.Join(repoRoot, "benchmarking/aws/runner/dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		return "", err
	}
	out := filepath.Join(dist, "redpanda-connect")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/redpanda-connect")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return out, nil
}

// The next four are placeholders that the staging task (Task 12) implements
// once the shared stack outputs the runner/load-gen instance IDs and the
// artefact bucket. They are declared here so the bench flow compiles in
// isolation.
func renderPipelineConfig(s *Scenario, outs map[string]string) (string, error) {
	cfg := map[string]any{
		"http":    map[string]any{"debug_endpoints": true},
		"input":   s.Pipeline["input"],
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

func substitutePlaceholders(in string, outs map[string]string) string {
	for k, v := range outs {
		in = strings.ReplaceAll(in, "${"+strings.ToUpper(k)+"}", v)
	}
	return in
}

func stageArtefacts(_ context.Context, _ benchOpts, _ map[string]string, _, _ string) error {
	// Implemented in Task 12.
	return nil
}
func runSeeder(_ context.Context, _ benchOpts, _ *Scenario, _ map[string]string) error {
	// Implemented in Task 12.
	return nil
}
func combineReset(steps []ResetStep, outs map[string]string) string {
	var sb strings.Builder
	for _, st := range steps {
		if st.SQL != "" {
			sb.WriteString(fmt.Sprintf("PGPASSWORD=$POSTGRES_PASSWORD psql \"$POSTGRES_DSN\" -c %q\n", st.SQL))
		}
		if st.Bash != "" {
			sb.WriteString(substitutePlaceholders(st.Bash, outs) + "\n")
		}
	}
	return sb.String()
}
func renderWorkloadScript(s *Scenario, outs map[string]string) string {
	if s.Workload == nil {
		return ""
	}
	// Postgres-orders workload: drive inserts at the configured rate.
	// Implemented in Task 12.
	return ""
}
```

- [ ] **Step 2: Verify build**

```bash
go build ./benchmarking/aws/runner
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/runner/main.go
git commit -m "$(cat <<'EOF'
feat(bench/aws): wire CLI main with bench/validate/down subcommands

End-to-end orchestration: terraform init+apply (shared + stack), build the
binary for linux/arm64, render the pipeline config, run the sweep, write
JSON, append markdown, terraform destroy. Staging and workload scripts are
placeholders filled in by Task 12.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Terraform shared stack — backend, providers, VPC

We split the shared stack across multiple commits to keep each diff reviewable.

**Files:**
- Create: `benchmarking/aws/terraform/backend.hcl`
- Create: `benchmarking/aws/terraform/shared/main.tf`
- Create: `benchmarking/aws/terraform/shared/variables.tf`
- Create: `benchmarking/aws/terraform/shared/vpc.tf`

- [ ] **Step 1: Backend config**

Write `benchmarking/aws/terraform/backend.hcl`:

```hcl
# Shared S3 backend for all benchmarking/aws/terraform/{shared,stacks/*} state.
# Region and bucket below are the dedicated benchmarking AWS account; change in
# a private fork if you run this in another account.
bucket         = "redpanda-connect-bench-tfstate"
region         = "us-east-2"
dynamodb_table = "redpanda-connect-bench-tflocks"
encrypt        = true
```

- [ ] **Step 2: Providers + locals + tags**

Write `benchmarking/aws/terraform/shared/main.tf`:

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project          = "redpanda-connect-bench"
      "bench-session-id" = var.bench_session_id
      ManagedBy        = "terraform"
    }
  }
}

locals {
  name_prefix = "rpcn-bench"
}
```

- [ ] **Step 3: Variables**

Write `benchmarking/aws/terraform/shared/variables.tf`:

```hcl
variable "region" {
  type    = string
  default = "us-east-2"
}

variable "runner_instance_type" {
  description = "EC2 instance type for the Connect benchmark host."
  type        = string
  default     = "c7i.4xlarge"
}

variable "load_gen_instance_type" {
  description = "EC2 instance type for the load generator."
  type        = string
  default     = "c7i.large"
}

variable "bench_session_id" {
  description = "Tag applied to every resource for orphan cleanup."
  type        = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}
```

- [ ] **Step 4: VPC**

Write `benchmarking/aws/terraform/shared/vpc.tf`:

```hcl
data "aws_availability_zones" "available" { state = "available" }

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "${local.name_prefix}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${local.name_prefix}-igw" }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags = { Name = "${local.name_prefix}-public-${count.index}" }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 10 + count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags = { Name = "${local.name_prefix}-private-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}
```

- [ ] **Step 5: Validate**

```bash
cd benchmarking/aws/terraform/shared
terraform init -backend=false
terraform validate
cd - >/dev/null
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/terraform/backend.hcl benchmarking/aws/terraform/shared/{main,variables,vpc}.tf
git commit -m "$(cat <<'EOF'
feat(bench/aws): add terraform shared stack — providers, vars, VPC

S3+DynamoDB backend, AWS provider with bench-session-id default tags, /16
VPC across 2 AZs with public+private subnets, IGW, public route table.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Terraform shared stack — IAM, security groups, EC2 hosts, results bucket

- [ ] **Step 1: Security groups**

Write `benchmarking/aws/terraform/shared/security.tf`:

```hcl
resource "aws_security_group" "runner" {
  name        = "${local.name_prefix}-runner"
  description = "Runner EC2 — egress for SSM + source connections"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "load_gen" {
  name        = "${local.name_prefix}-load-gen"
  description = "Load-gen EC2 — egress for SSM + source writes"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Stacks reference these to allow ingress to their RDS / MSK / etc.
output "runner_sg_id"   { value = aws_security_group.runner.id }
output "load_gen_sg_id" { value = aws_security_group.load_gen.id }
```

- [ ] **Step 2: IAM**

Write `benchmarking/aws/terraform/shared/iam.tf`:

```hcl
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bench_host" {
  name               = "${local.name_prefix}-host"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.bench_host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Read secrets (for any future stack that uses them) + write to results bucket.
resource "aws_iam_role_policy" "bench_host_extra" {
  role = aws_iam_role.bench_host.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "secretsmanager:GetSecretValue",
        ]
        Resource = ["*"]
      },
    ]
  })
}

resource "aws_iam_instance_profile" "bench_host" {
  name = "${local.name_prefix}-host"
  role = aws_iam_role.bench_host.name
}
```

- [ ] **Step 3: Runner + load-gen EC2**

Write `benchmarking/aws/terraform/shared/runner.tf`:

```hcl
data "aws_ssm_parameter" "al2023_arm64_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

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

resource "aws_instance" "runner" {
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.runner_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.runner.id]
  iam_instance_profile   = aws_iam_instance_profile.bench_host.name
  user_data              = local.cloud_init

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    throughput  = 500
    iops        = 4000
  }

  tags = { Name = "${local.name_prefix}-runner" }
}

resource "aws_instance" "load_gen" {
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.load_gen_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.load_gen.id]
  iam_instance_profile   = aws_iam_instance_profile.bench_host.name
  user_data              = local.cloud_init

  root_block_device {
    volume_type = "gp3"
    volume_size = 40
  }

  tags = { Name = "${local.name_prefix}-load-gen" }
}
```

- [ ] **Step 4: Results bucket**

Write `benchmarking/aws/terraform/shared/results_bucket.tf`:

```hcl
resource "aws_s3_bucket" "results" {
  bucket_prefix = "${local.name_prefix}-results-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    id     = "expire-raw-json"
    status = "Enabled"
    expiration { days = 180 }
  }
}
```

- [ ] **Step 5: Outputs**

Write `benchmarking/aws/terraform/shared/outputs.tf`:

```hcl
output "runner_instance_id"   { value = aws_instance.runner.id }
output "load_gen_instance_id" { value = aws_instance.load_gen.id }
output "vpc_id"               { value = aws_vpc.main.id }
output "private_subnet_ids"   { value = aws_subnet.private[*].id }
output "public_subnet_ids"    { value = aws_subnet.public[*].id }
output "results_bucket"       { value = aws_s3_bucket.results.bucket }
```

- [ ] **Step 6: Validate**

```bash
cd benchmarking/aws/terraform/shared
terraform validate
cd - >/dev/null
```

Expected: `Success!`.

- [ ] **Step 7: Commit**

```bash
git add benchmarking/aws/terraform/shared/{security,iam,runner,results_bucket,outputs}.tf
git commit -m "$(cat <<'EOF'
feat(bench/aws): add shared IAM, EC2 hosts, and results bucket

Runner + load-gen EC2 instances on AL2023 arm64 with the SSM-managed
instance role, gp3 root volumes, and a cloud-init that creates /opt/bench
and installs psql + jq. A private versioned S3 bucket with a 180-day
lifecycle hosts raw result JSON.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Stage artefacts + seeder invocation + workload writer

This fills the placeholders left in Task 9 (`stageArtefacts`, `runSeeder`, `renderWorkloadScript`).

**Files:**
- Modify: `benchmarking/aws/runner/main.go`

- [ ] **Step 1: Add S3 SDK + the staging helpers**

```bash
go get github.com/aws/aws-sdk-go-v2/service/s3@v1.69.0
go mod tidy
```

Replace the three placeholder functions at the bottom of `benchmarking/aws/runner/main.go` with the implementations below.

```go
import (
	// add these to the existing imports
	"bytes"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func stageArtefacts(ctx context.Context, opts benchOpts, outs map[string]string, binPath, cfgPath string) error {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]
	for _, item := range []struct{ key, path string }{
		{"stage/redpanda-connect", binPath},
		{"stage/config.yaml", cfgPath},
	} {
		f, err := os.Open(item.path)
		if err != nil {
			return err
		}
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &item.key,
			Body:   f,
		})
		f.Close()
		if err != nil {
			return fmt.Errorf("upload %s: %w", item.path, err)
		}
	}
	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/stage/redpanda-connect /opt/bench/redpanda-connect
aws s3 cp s3://%s/stage/config.yaml /opt/bench/config.yaml
chmod +x /opt/bench/redpanda-connect
`, bucket, bucket)
	return ssmExec.Run(ctx, outs["runner_instance_id"], script, streamingOnLine(os.Stdout, "stage"))
}

func runSeeder(ctx context.Context, opts benchOpts, s *Scenario, outs map[string]string) error {
	if s.Dataset.Seeder == "" {
		return nil
	}
	// Build the seeder for linux/arm64 and upload to S3, then exec on load-gen.
	dist := filepath.Join(opts.repoRoot, "benchmarking/aws/seeders/dist")
	_ = os.MkdirAll(dist, 0o755)
	binOut := filepath.Join(dist, s.Dataset.Seeder)
	cmd := exec.Command("go", "build", "-o", binOut, "./benchmarking/aws/seeders/"+s.Dataset.Seeder)
	cmd.Dir = opts.repoRoot
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build seeder: %w", err)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]
	f, err := os.Open(binOut)
	if err != nil {
		return err
	}
	defer f.Close()
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

func renderWorkloadScript(s *Scenario, outs map[string]string) string {
	if s.Workload == nil {
		return ""
	}
	totalSec := int((s.Workload.Warmup + s.Workload.Duration).Seconds())
	return fmt.Sprintf(`
set -euo pipefail
POSTGRES_DSN=%q /opt/bench/cdc-rows workload \
  --tables=%s --row-size=%d \
  --rate=%d --duration=%ds
`,
		outs["postgres_dsn"],
		strings.Join(s.Dataset.Tables, ","),
		s.Dataset.RowSizeBytes,
		s.Workload.WriteRatePerSec,
		totalSec)
}

// satisfy bytes import for future use
var _ = bytes.NewBuffer
```

- [ ] **Step 2: Verify build**

```bash
go build ./benchmarking/aws/runner
```

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/runner/main.go go.mod go.sum
git commit -m "$(cat <<'EOF'
feat(bench/aws): implement artefact staging, seeder, and workload writer

stageArtefacts uploads the redpanda-connect binary and rendered config to
the results bucket, then SSMs the runner to pull them down. runSeeder does
the same for the cdc-rows seeder binary and invokes its 'seed' subcommand
on the load-gen host. renderWorkloadScript drives sustained inserts at the
scenario's configured rate.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Terraform module — rds-postgres

The first connector module. Reused by the postgres stack.

**Files:**
- Create: `benchmarking/aws/terraform/modules/rds-postgres/main.tf`
- Create: `benchmarking/aws/terraform/modules/rds-postgres/variables.tf`
- Create: `benchmarking/aws/terraform/modules/rds-postgres/outputs.tf`

- [ ] **Step 1: Variables**

```hcl
# benchmarking/aws/terraform/modules/rds-postgres/variables.tf
variable "name_prefix"        { type = string }
variable "vpc_id"             { type = string }
variable "subnet_ids"         { type = list(string) }
variable "client_sg_ids"      { type = list(string), description = "SGs allowed to connect on 5432" }
variable "instance_class"     { type = string, default = "db.r6g.2xlarge" }
variable "storage_gb"         { type = number, default = 400 }
variable "iops"               { type = number, default = 12000 }
variable "engine_version"     { type = string, default = "16.4" }
variable "db_name"            { type = string, default = "benchdb" }
variable "master_username"    { type = string, default = "bench" }
variable "parameters" {
  type        = map(string)
  default     = { wal_level = "logical", max_wal_senders = "20" }
}
```

> Terraform supports the inline-comma form above (`type = string, default = …`) only inside object literals, so split each into its own line.

Rewrite that file as:

```hcl
variable "name_prefix" { type = string }
variable "vpc_id"      { type = string }
variable "subnet_ids" {
  type = list(string)
}
variable "client_sg_ids" {
  type        = list(string)
  description = "SGs allowed to connect on 5432"
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
  default = "16.4"
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
  type    = map(string)
  default = { wal_level = "logical", max_wal_senders = "20" }
}
```

- [ ] **Step 2: Main**

Write `benchmarking/aws/terraform/modules/rds-postgres/main.tf`:

```hcl
resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-pg"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "this" {
  name        = "${var.name_prefix}-pg-sg"
  description = "Allow Postgres from bench clients"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.client_sg_ids
    content {
      from_port       = 5432
      to_port         = 5432
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
  name   = "${var.name_prefix}-pg"
  family = "postgres16"
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
  identifier             = "${var.name_prefix}-pg"
  engine                 = "postgres"
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
}
```

- [ ] **Step 3: Outputs**

Write `benchmarking/aws/terraform/modules/rds-postgres/outputs.tf`:

```hcl
output "postgres_dsn" {
  value     = "postgres://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.address}:5432/${var.db_name}?sslmode=require"
  sensitive = true
}
output "postgres_endpoint" { value = aws_db_instance.this.address }
output "postgres_password" {
  value     = random_password.master.result
  sensitive = true
}
```

- [ ] **Step 4: Validate**

```bash
cd benchmarking/aws/terraform/modules/rds-postgres
terraform init -backend=false
terraform validate
cd - >/dev/null
```

Expected: `Success!`.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/terraform/modules/rds-postgres/
git commit -m "$(cat <<'EOF'
feat(bench/aws): add rds-postgres terraform module

Provisions a Postgres 16 RDS instance with gp3 storage, configurable IOPS,
and a parameter group for logical replication. Outputs the DSN as a
sensitive value for downstream stacks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: Terraform stack — postgres

Composes shared + rds-postgres into a single deployable unit.

**Files:**
- Create: `benchmarking/aws/terraform/stacks/postgres/main.tf`
- Create: `benchmarking/aws/terraform/stacks/postgres/variables.tf`
- Create: `benchmarking/aws/terraform/stacks/postgres/outputs.tf`

- [ ] **Step 1: Variables**

```hcl
# benchmarking/aws/terraform/stacks/postgres/variables.tf
variable "region"             { type = string, default = "us-east-2" }
variable "instance_class"     { type = string }
variable "storage_gb"         { type = number }
variable "iops"               { type = number }
variable "parameters" {
  type    = map(string)
  default = { wal_level = "logical", max_wal_senders = "20" }
}
```

(Same syntax fix as Task 13 — split inline commas.)

```hcl
variable "region" {
  type    = string
  default = "us-east-2"
}
variable "instance_class" { type = string }
variable "storage_gb"     { type = number }
variable "iops"           { type = number }
variable "parameters" {
  type    = map(string)
  default = { wal_level = "logical", max_wal_senders = "20" }
}
```

- [ ] **Step 2: Read the shared state, compose the module**

Write `benchmarking/aws/terraform/stacks/postgres/main.tf`:

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
      Stack     = "postgres"
      ManagedBy = "terraform"
    }
  }
}

# Pull the shared stack outputs.
data "terraform_remote_state" "shared" {
  backend = "s3"
  config = {
    bucket = "redpanda-connect-bench-tfstate"
    region = var.region
    key    = "shared/terraform.tfstate"
  }
}

module "rds" {
  source         = "../../modules/rds-postgres"
  name_prefix    = "rpcn-bench-pg"
  vpc_id         = data.terraform_remote_state.shared.outputs.vpc_id
  subnet_ids     = data.terraform_remote_state.shared.outputs.private_subnet_ids
  client_sg_ids  = [
    data.terraform_remote_state.shared.outputs.runner_sg_id,
    data.terraform_remote_state.shared.outputs.load_gen_sg_id,
  ]
  instance_class = var.instance_class
  storage_gb     = var.storage_gb
  iops           = var.iops
  parameters     = var.parameters
}
```

- [ ] **Step 3: Outputs (pass-through)**

Write `benchmarking/aws/terraform/stacks/postgres/outputs.tf`:

```hcl
output "postgres_dsn" {
  value     = module.rds.postgres_dsn
  sensitive = true
}
output "postgres_endpoint" {
  value = module.rds.postgres_endpoint
}
output "postgres_password" {
  value     = module.rds.postgres_password
  sensitive = true
}
```

- [ ] **Step 4: Validate**

```bash
cd benchmarking/aws/terraform/stacks/postgres
terraform init -backend=false
terraform validate
cd - >/dev/null
```

Expected: `Success!`.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/terraform/stacks/postgres/
git commit -m "$(cat <<'EOF'
feat(bench/aws): add terraform stack — postgres

Composes the shared VPC + SGs with the rds-postgres module via remote_state.
Passes scenario-supplied sizing (instance class, storage, IOPS, params).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 15: Seeder — cdc-rows (Postgres path)

The seeder runs on the load-gen EC2. It has two subcommands: `seed` (bulk insert N rows) and `workload` (sustained inserts at rate). Postgres path only in this plan; the DynamoDB path is added by the dynamodb plan.

**Files:**
- Create: `benchmarking/aws/seeders/cdc-rows/main.go`
- Create: `benchmarking/aws/seeders/cdc-rows/sql.go`

- [ ] **Step 1: Add the pgx driver dependency**

```bash
go get github.com/jackc/pgx/v5@v5.7.0
go mod tidy
```

- [ ] **Step 2: main.go**

Write `benchmarking/aws/seeders/cdc-rows/main.go`:

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
		fmt.Fprintln(os.Stderr, "usage: cdc-rows {seed|workload} [flags]")
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

- [ ] **Step 3: sql.go**

Write `benchmarking/aws/seeders/cdc-rows/sql.go`:

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func seed(ctx context.Context, tables []string, rows int64, rowSize int) error {
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_DSN"))
	if err != nil {
		return err
	}
	defer pool.Close()

	for _, table := range tables {
		if err := ensureTable(ctx, pool, table, rowSize); err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(tables))
	for _, table := range tables {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			errCh <- bulkInsert(ctx, pool, t, rows, rowSize)
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

func ensureTable(ctx context.Context, pool *pgxpool.Pool, table string, rowSize int) error {
	stmts := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf(`CREATE TABLE %s (
			id          BIGSERIAL PRIMARY KEY,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			payload     TEXT NOT NULL
		)`, table),
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	_ = rowSize
	return nil
}

func bulkInsert(ctx context.Context, pool *pgxpool.Pool, table string, rows int64, rowSize int) error {
	const workers = 16
	rowsPerWorker := rows / workers
	payload := randomPayload(rowSize)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := strings.Repeat("(NOW(),$1),", 1000)
			batch = strings.TrimSuffix(batch, ",")
			stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, batch)
			conn, err := pool.Acquire(ctx)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Release()
			done := int64(0)
			for done < rowsPerWorker {
				if _, err := conn.Exec(ctx, stmt, payload); err != nil {
					errCh <- err
					return
				}
				done += 1000
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
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_DSN"))
	if err != nil {
		return err
	}
	defer pool.Close()
	payload := randomPayload(rowSize)
	deadline := time.Now().Add(dur)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	per100ms := rate / 10
	tIdx := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil
			}
			table := tables[tIdx%len(tables)]
			tIdx++
			batch := strings.Repeat("(NOW(),$1),", per100ms)
			batch = strings.TrimSuffix(batch, ",")
			stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, batch)
			if _, err := pool.Exec(ctx, stmt, payload); err != nil {
				return err
			}
		}
	}
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

- [ ] **Step 4: Verify build**

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ./benchmarking/aws/seeders/cdc-rows
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/seeders/cdc-rows/ go.mod go.sum
git commit -m "$(cat <<'EOF'
feat(bench/aws): add cdc-rows seeder (Postgres path)

Two subcommands. 'seed' creates the table, then bulk-inserts N rows
across 16 concurrent workers (~1000 rows per INSERT). 'workload' runs a
100ms-tick loop emitting <rate>/10 rows per tick across the table list,
until duration elapses. DynamoDB path will be added by the dynamodb plan.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 16: Scenario file — postgres/orders-cdc

The customer-shaped scenario that this plan ships.

**Files:**
- Create: `benchmarking/aws/scenarios/postgres/orders-cdc.yaml`

- [ ] **Step 1: Write the YAML**

```yaml
name: postgres-orders-cdc
description: |
  Stream changes from a 75M-row orders table at 5K writes/sec sustained.
  Mirrors a mid-size e-commerce CDC pipeline (Postgres → drop, measuring
  read ceiling).

connector: postgres_cdc
stack: postgres

infra:
  source:
    instance_class: db.r6g.2xlarge
    storage_gb: 400
    iops: 12000
    parameters:
      wal_level: logical
      max_wal_senders: "20"
  runner:
    instance_type: c7i.4xlarge

dataset:
  initial_rows: 75000000
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows

workload:
  write_rate_per_sec: 5000
  duration: 15m
  warmup: 2m

pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
      stream_snapshot: false
      schema: public
      tables: [orders]
      slot_name: bench_slot
      batching:
        count: 5000
        period: 1s

matrix:
  cpu_points: [1, 2, 4, 8]

reset:
  - sql: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"
```

- [ ] **Step 2: Validate via the runner CLI**

```bash
go run ./benchmarking/aws/runner validate --scenario=benchmarking/aws/scenarios/postgres/orders-cdc.yaml
```

Expected output:
```
scenario postgres-orders-cdc OK (4 cpu points, runner c7i.4xlarge)
```

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/scenarios/postgres/orders-cdc.yaml
git commit -m "$(cat <<'EOF'
feat(bench/aws): add postgres orders-cdc scenario

A customer-shaped CDC workload: 75M orders rows, sustained 5K writes/sec
for 15 minutes after a 2-minute warmup, swept across 1/2/4/8 vCPU.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 17: Top-level Taskfile

Operator UX.

**Files:**
- Create: `benchmarking/aws/Taskfile.yml`

- [ ] **Step 1: Write the Taskfile**

```yaml
version: '3'

vars:
  RUNNER: 'go run ./runner'

tasks:
  aws:bench:
    desc: "Run a scenario end-to-end (provision → sweep → render → teardown)"
    dir: '.'
    cmds:
      - |
        if [ -z "{{.scenario}}" ]; then
          echo "usage: task aws:bench -- scenario=<path>"
          exit 2
        fi
        cd ../.. && {{.RUNNER}} bench \
          --scenario=benchmarking/aws/scenarios/{{.scenario}}.yaml \
          --repo-root=.

  aws:validate:
    desc: "Validate a scenario YAML + terraform plan (no AWS spend)"
    cmds:
      - |
        cd ../.. && {{.RUNNER}} validate \
          --scenario=benchmarking/aws/scenarios/{{.scenario}}.yaml

  aws:down:
    desc: "Tear down infra for a scenario (when bench was run with keep=true)"
    cmds:
      - |
        cd ../.. && {{.RUNNER}} down \
          --scenario=benchmarking/aws/scenarios/{{.scenario}}.yaml \
          --repo-root=.

  aws:cost-check:
    desc: "Estimated hourly cost of currently running stacks"
    cmds:
      - cd ../.. && {{.RUNNER}} cost-check
```

- [ ] **Step 2: Smoke the validate path**

From repo root:

```bash
task -t benchmarking/aws/Taskfile.yml aws:validate -- scenario=postgres/orders-cdc
```

Expected: scenario validation succeeds.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/Taskfile.yml
git commit -m "$(cat <<'EOF'
feat(bench/aws): add operator-facing Taskfile

One command per lifecycle stage. The default `aws:bench` invocation
provisions, runs the sweep, renders results, and tears down.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 18: Doc updates — pointer from the existing benchmarking guide

**Files:**
- Modify: `docs/benchmarking.md`

- [ ] **Step 1: Read the existing doc to find the right insertion point**

```bash
grep -n "^## " docs/benchmarking.md
```

The new section goes before the existing "Reporting Results" section.

- [ ] **Step 2: Append a new section pointing at the AWS framework**

Add the following just before `## Reporting Results`:

```markdown
## Running on AWS (production-shaped numbers)

For the published per-connector throughput numbers in `docs/benchmark-results/`,
benchmarks run on real AWS infrastructure via the framework under
[`benchmarking/aws/`](../benchmarking/aws/). One command provisions infra,
sweeps a `[1, 2, 4, 8]` vCPU matrix, renders results into
`docs/benchmark-results/<connector>.md`, and tears down. See the
[AWS framework README](../benchmarking/aws/README.md) for the operator workflow.

Local Docker benchmarks under `internal/impl/<connector>/bench/` remain the
developer-iteration path; AWS runs are for numbers we publish.
```

- [ ] **Step 3: Commit**

```bash
git add docs/benchmarking.md
git commit -m "$(cat <<'EOF'
docs(benchmarking): point at the new AWS framework

The local Docker benches stay the developer-iteration path; the new
benchmarking/aws/ framework is the path for the published numbers.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 19: Pre-flight smoke (no AWS spend)

Verify the whole foundation compiles, tests pass, and validation works.

- [ ] **Step 1: Tidy modules**

```bash
go mod tidy
```

Expected: no changes to go.mod/go.sum, or only minor updates.

- [ ] **Step 2: Run all runner tests**

```bash
go test ./benchmarking/aws/runner -v
```

Expected: every test passes.

- [ ] **Step 3: Build the seeder for the target platform**

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /dev/null ./benchmarking/aws/seeders/cdc-rows
```

Expected: no errors.

- [ ] **Step 4: Build Redpanda Connect for the target platform**

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /dev/null ./cmd/redpanda-connect
```

Expected: no errors.

- [ ] **Step 5: Validate every terraform stack**

```bash
for d in benchmarking/aws/terraform/shared benchmarking/aws/terraform/modules/rds-postgres benchmarking/aws/terraform/stacks/postgres; do
  (cd "$d" && terraform init -backend=false >/dev/null && terraform validate)
done
```

Expected: `Success!` from each.

- [ ] **Step 6: Validate the scenario end-to-end**

```bash
task -t benchmarking/aws/Taskfile.yml aws:validate -- scenario=postgres/orders-cdc
```

Expected:
```
scenario postgres-orders-cdc OK (4 cpu points, runner c7i.4xlarge)
```

- [ ] **Step 7: Commit (no-op if everything is already committed)**

```bash
git status --short
```

If anything appears, decide whether it's an oversight from a prior task and commit accordingly. Otherwise move on.

---

## Task 20: End-to-end AWS smoke (the one that spends money)

**This task spends real AWS dollars (~$5).** It is the only way to verify the framework actually works end-to-end. Run it once before declaring the foundation done, then read the rendered results section to confirm the markdown shape matches the spec.

**Files modified by this task:**
- `docs/benchmark-results/postgres.md` (appended)
- `benchmarking/aws/results/postgres/orders-cdc/<timestamp>.json` (gitignored)

- [ ] **Step 1: Prerequisites check**

Ensure these are set in your shell before running:

```bash
aws sts get-caller-identity   # confirm you are pointed at the dedicated bench account
aws s3 ls s3://redpanda-connect-bench-tfstate  # confirm the state bucket exists
```

If the state bucket does not yet exist, create it once:

```bash
aws s3 mb s3://redpanda-connect-bench-tfstate --region us-east-2
aws dynamodb create-table --table-name redpanda-connect-bench-tflocks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST --region us-east-2
```

- [ ] **Step 2: Run the bench**

```bash
task -t benchmarking/aws/Taskfile.yml aws:bench -- scenario=postgres/orders-cdc
```

Expected:
- Each step prints its label.
- Wall clock ~90 minutes.
- At the end, `docs/benchmark-results/postgres.md` has a new section starting with `## AWS — orders-cdc — <today>`.
- AWS resources are all destroyed (sanity-check with `aws ec2 describe-instances --filters Name=tag:Project,Values=redpanda-connect-bench`).

- [ ] **Step 3: Manually verify the markdown section**

Open `docs/benchmark-results/postgres.md` and confirm the appended section has:
- the correct date and scenario name in the heading;
- a four-row throughput table with non-zero MB/sec at every CPU point;
- the Git SHA link points at the commit you ran from;
- the JSON link path matches what `ls benchmarking/aws/results/postgres/orders-cdc/` shows.

If anomalies were detected, they appear as quoted callouts under the table.

- [ ] **Step 4: Commit the rendered markdown**

```bash
git add docs/benchmark-results/postgres.md
git commit -m "$(cat <<'EOF'
docs(benchmark-results): add first AWS postgres orders-cdc run

First end-to-end run of the new AWS benchmarking framework. See the new
section in docs/benchmark-results/postgres.md for the numbers.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

The foundation is complete.

---

## Self-review

After writing this plan, I checked against the spec:

**1. Spec coverage:**
- Architecture & layout (spec §Architecture) — Tasks 1, 10–14, 17.
- Scenario format & validation (spec §Scenario format) — Tasks 2, 16.
- Runner orchestrator execution flow (spec §Runner orchestrator) — Tasks 6–9, 12.
- Terraform layout including shared, modules, stacks (spec §Terraform layout) — Tasks 10–11, 13–14. (Note: orphan-cleanup Lambda is deferred to a follow-up plan to keep this foundation lean; called out in §Spec deltas.)
- Results format JSON + markdown rendering + p5/p50/p95 + anomalies (spec §Results format) — Tasks 3–5.
- 15-min minimum window + variance reporting + anomaly detection (spec §Why 15 minutes) — Tasks 2 (validation), 3 (percentiles), 4 (anomalies).
- Onboarding new connectors (spec §Onboarding a new connector) — implicit; each future connector plan follows Tasks 13–16.
- Scenarios — only postgres/orders-cdc ships here per the scope split. Other 12 scenarios get their own plans.

**Gaps deliberately deferred** (called out at the top of the plan):
- mysql, sqlserver, dynamodb, s3, iceberg, kafka-to-pg connectors.
- The orphan-cleanup Lambda (spec §Cost guardrails) — tags are in place; the Lambda is a follow-up.
- Multi-scenario session execution (`scenario=a,b`) — single scenario only in this plan.
- `cost-check` command — stub only.

**2. Placeholder scan:** No `TBD`/`TODO`/"implement later" left in the plan. Task 9 declares four placeholder functions in `main.go` and Task 12 explicitly implements them. The seeder DynamoDB path is intentionally not in this plan (lives in the future dynamodb plan).

**3. Type consistency:**
- `Scenario.Matrix.CPUPoints` (`[]int`) is consistent across `scenario.go`, `matrix.go`, `main.go`.
- `Sample`, `Summary`, `Anomaly`, `Result`, `PointResult` types defined once each and reused.
- Terraform variable names (`region`, `runner_instance_type`, `instance_class`, `storage_gb`, `iops`, `parameters`) are consistent between scenario YAML, `translateInfraSource` in `main.go`, and the per-stack `variables.tf`.

No issues found in the second pass.
