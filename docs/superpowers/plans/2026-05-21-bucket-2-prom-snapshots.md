# Bucket 2 — Prometheus snapshot capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** During each sweep point, a background sidecar on the runner scrapes Connect's `localhost:4195/metrics` every 10s and uploads timestamped snapshots to S3. The orchestrator downloads the dump, parses a curated subset (`go_goroutines`, `go_memstats_heap_inuse_bytes`, `go_memstats_gc_pause_total_ns`, `process_cpu_seconds_total`, `benchmark_bytes_total`) into a `[]PromPoint` time series, embeds it in `PointResult.Prom`, and uses it to annotate detected anomalies with goroutine / heap context.

**Architecture:** Add a `prom.go` parser to the runner package (hand-rolled, no new external deps). Extend `renderBenchScript` with a third background subshell that scrapes /metrics. Extend `MatrixRunner.Run` to fetch the prom dump from S3 alongside the existing log fetch. Extend `PointResult` with `Prom []PromPoint` and `Anomaly` with optional `GoroutinesAtStart`, `HeapInUseMBAtStart`, `GCPauseDeltaNS` fields populated from the snapshot nearest each anomaly's start.

**Tech Stack:** Go stdlib only (no `github.com/prometheus/common/expfmt`), `testify` for tests.

**Spec:** [`docs/superpowers/specs/2026-05-21-bucket-2-foundation-polish-design.md`](../specs/2026-05-21-bucket-2-foundation-polish-design.md) — Item 3.

**Prerequisites:** Connect already serves Prometheus metrics on `localhost:4195/metrics` via the existing pipeline config in `renderPipelineConfig`. No new infra.

**Out of scope:** Node-level (OS) metrics. Histogram / summary metric decode. Label parsing.

---

## File Structure

```
benchmarking/aws/runner/
├── prom.go                    # NEW — PromPoint, parser, curated extractor
├── prom_test.go               # NEW
├── testdata/
│   └── prom-sample.txt        # NEW — captured /metrics from a real Connect run (~50KB)
├── matrix.go                  # MODIFY — scrape sidecar in bench script + fetchProm
├── render.go                  # MODIFY — PointResult.Prom
├── anomalies.go               # MODIFY — Anomaly prom-context fields + populator
├── anomalies_test.go          # MODIFY — assert new fields populate correctly
└── matrix_test.go             # MODIFY — assert prom fetch + embed in points
```

## Conventions

- License header on every new Go file.
- All AWS-touching code uses the existing `LogFetcher` interface — the prom dump is just another S3 object.

---

## Task 1: Capture a real /metrics fixture

**Files:**
- Create: `benchmarking/aws/runner/testdata/prom-sample.txt`

This is a one-time data-capture step done by the operator with a local Connect running the bench pipeline.

- [ ] **Step 1: Start Connect locally with the bench pipeline**

In one terminal:

```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
cat > /tmp/bench-local.yaml <<'EOF'
http:
  debug_endpoints: true
input:
  generate:
    interval: 10ms
    mapping: 'root = "hello"'
output:
  processors:
    - benchmark:
        interval: 1s
        count_bytes: true
  drop: {}
metrics:
  prometheus:
    add_process_metrics: true
    add_go_metrics: true
EOF
go run ./cmd/redpanda-connect run /tmp/bench-local.yaml
```

- [ ] **Step 2: Wait ~30s, then capture /metrics**

In another terminal:

```bash
mkdir -p benchmarking/aws/runner/testdata
curl -s http://localhost:4195/metrics > benchmarking/aws/runner/testdata/prom-sample.txt
```

- [ ] **Step 3: Stop Connect (Ctrl-C in the first terminal) and verify the fixture**

```bash
wc -l benchmarking/aws/runner/testdata/prom-sample.txt
grep -c '^go_goroutines ' benchmarking/aws/runner/testdata/prom-sample.txt
grep -c '^go_memstats_heap_inuse_bytes ' benchmarking/aws/runner/testdata/prom-sample.txt
grep -c '^process_cpu_seconds_total ' benchmarking/aws/runner/testdata/prom-sample.txt
grep -c '^benchmark_bytes_total ' benchmarking/aws/runner/testdata/prom-sample.txt
```

Expected: each `grep -c` returns `1` (one occurrence of each curated metric).
If any metric is missing, the pipeline config did not enable it. Re-check `add_process_metrics` and `add_go_metrics`.

- [ ] **Step 4: Commit the fixture**

```bash
git add benchmarking/aws/runner/testdata/prom-sample.txt
git commit -m "test(bench/aws): capture /metrics fixture from a local Connect run"
```

---

## Task 2: PromPoint type + snapshot framing parser (TDD)

**Files:**
- Create: `benchmarking/aws/runner/prom.go`
- Create: `benchmarking/aws/runner/prom_test.go`

- [ ] **Step 1: Create prom.go skeleton with types**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

// PromPoint is one curated snapshot of Connect's runtime metrics, sampled at
// time T (seconds since end-of-warmup, matching Sample.T).
type PromPoint struct {
	T               int     `json:"t"`
	Goroutines      int     `json:"goroutines"`
	HeapInUseMB     float64 `json:"heap_in_use_mb"`
	BytesTotal      float64 `json:"bytes_total"`       // benchmark_bytes_total
	CPUSeconds      float64 `json:"cpu_seconds"`
	GCPauseTotalNS  uint64  `json:"gc_pause_total_ns"` // monotonic; per-interval delta = scrape[i] - scrape[i-1]
}

// promSnapshot is one /metrics dump bracketed by ###timestamp= markers.
// The parser produces a slice of these before the curated extractor folds
// them into []PromPoint.
type promSnapshot struct {
	UnixTime int64
	Body     string
	Errored  bool // true when the scraper logged ###scrape_error inside the snapshot
}
```

- [ ] **Step 2: Add failing tests for snapshot framing**

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

func TestParseSnapshots_TwoFrames(t *testing.T) {
	raw := `###timestamp=1747856130
go_goroutines 312
process_cpu_seconds_total 87.4
###timestamp=1747856140
go_goroutines 314
process_cpu_seconds_total 92.1
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 2)
	require.Equal(t, int64(1747856130), snaps[0].UnixTime)
	require.Contains(t, snaps[0].Body, "go_goroutines 312")
	require.False(t, snaps[0].Errored)
	require.Equal(t, int64(1747856140), snaps[1].UnixTime)
	require.Contains(t, snaps[1].Body, "go_goroutines 314")
}

func TestParseSnapshots_ErrorFrame(t *testing.T) {
	raw := `###timestamp=1747856130
go_goroutines 312
###timestamp=1747856140
###scrape_error
###timestamp=1747856150
go_goroutines 314
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 3)
	require.False(t, snaps[0].Errored)
	require.True(t, snaps[1].Errored, "frame with ###scrape_error marked")
	require.False(t, snaps[2].Errored)
}

func TestParseSnapshots_IgnoresLeadingNoise(t *testing.T) {
	// Some shells emit lines before the first ###timestamp= marker — ignore them.
	raw := `noisy line
another
###timestamp=100
go_goroutines 1
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 1)
	require.Equal(t, int64(100), snaps[0].UnixTime)
}

func TestParseSnapshots_TruncatedLastFrame(t *testing.T) {
	// Scraper got killed mid-write; last frame has no body.
	raw := `###timestamp=100
go_goroutines 1
###timestamp=200
`
	snaps := parseSnapshots(strings.NewReader(raw))
	require.Len(t, snaps, 2)
	require.Empty(t, strings.TrimSpace(snaps[1].Body))
}

func TestParseSnapshots_Empty(t *testing.T) {
	snaps := parseSnapshots(strings.NewReader(""))
	require.Empty(t, snaps)
}
```

- [ ] **Step 3: Run tests to verify fail**

Run: `go test ./benchmarking/aws/runner/ -run TestParseSnapshots -v`
Expected: FAIL — `parseSnapshots` undefined.

- [ ] **Step 4: Implement parseSnapshots in prom.go**

Append to `prom.go`:

```go
import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// parseSnapshots splits a /tmp/prom-N.txt dump on ###timestamp= markers.
// Anything before the first marker is ignored; an empty trailing frame
// (scraper killed mid-write) is kept with Body == "".
func parseSnapshots(r io.Reader) []promSnapshot {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var snaps []promSnapshot
	var current *promSnapshot
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "###timestamp=") {
			if current != nil {
				snaps = append(snaps, *current)
			}
			ts, _ := strconv.ParseInt(strings.TrimPrefix(line, "###timestamp="), 10, 64)
			current = &promSnapshot{UnixTime: ts}
			continue
		}
		if current == nil {
			continue // ignore noise before first marker
		}
		if line == "###scrape_error" {
			current.Errored = true
			continue
		}
		current.Body += line + "\n"
	}
	if current != nil {
		snaps = append(snaps, *current)
	}
	return snaps
}
```

- [ ] **Step 5: Run tests to verify pass**

Run: `go test ./benchmarking/aws/runner/ -run TestParseSnapshots -v`
Expected: PASS for all 5 cases.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/prom.go benchmarking/aws/runner/prom_test.go
git commit -m "feat(bench/aws): parseSnapshots — frame ###timestamp dumps"
```

---

## Task 3: Curated metric extractor (TDD)

**Files:**
- Modify: `benchmarking/aws/runner/prom.go`
- Modify: `benchmarking/aws/runner/prom_test.go`

- [ ] **Step 1: Add failing tests using the fixture**

Append to `prom_test.go`:

```go
import (
	"os"
	// ... existing
)

func TestExtractPromPoint_FromRealFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/prom-sample.txt")
	require.NoError(t, err)
	body := string(raw)

	pp, ok := extractPromPoint(promSnapshot{UnixTime: 1747856130, Body: body})
	require.True(t, ok)
	require.Greater(t, pp.Goroutines, 0)
	require.Greater(t, pp.HeapInUseMB, 0.0)
	require.GreaterOrEqual(t, pp.BytesTotal, 0.0)
	require.GreaterOrEqual(t, pp.CPUSeconds, 0.0)
	require.GreaterOrEqual(t, pp.GCPauseTotalNS, uint64(0))
}

func TestExtractPromPoint_SyntheticAllMetrics(t *testing.T) {
	body := `# HELP go_goroutines blah
# TYPE go_goroutines gauge
go_goroutines 312
go_memstats_heap_inuse_bytes 1.04857e+08
go_memstats_gc_pause_total_ns 4.2e+07
process_cpu_seconds_total 87.4
benchmark_bytes_total 4.12e+09
`
	pp, ok := extractPromPoint(promSnapshot{UnixTime: 100, Body: body})
	require.True(t, ok)
	require.Equal(t, 312, pp.Goroutines)
	require.InDelta(t, 100.0, pp.HeapInUseMB, 0.1)    // 1.04857e+08 B ≈ 100 MB
	require.InDelta(t, 4.12e+09, pp.BytesTotal, 1.0)
	require.InDelta(t, 87.4, pp.CPUSeconds, 0.01)
	require.Equal(t, uint64(42000000), pp.GCPauseTotalNS)
}

func TestExtractPromPoint_ErrorSnapshotSkipped(t *testing.T) {
	_, ok := extractPromPoint(promSnapshot{UnixTime: 100, Errored: true, Body: ""})
	require.False(t, ok)
}

func TestExtractPromPoint_PartialMetricsOK(t *testing.T) {
	body := `go_goroutines 50
go_memstats_heap_inuse_bytes 1.0485e+07
`
	pp, ok := extractPromPoint(promSnapshot{UnixTime: 1, Body: body})
	require.True(t, ok)
	require.Equal(t, 50, pp.Goroutines)
	require.InDelta(t, 10.0, pp.HeapInUseMB, 0.1)
	require.Equal(t, 0.0, pp.CPUSeconds)        // missing — zero is OK
	require.Equal(t, uint64(0), pp.GCPauseTotalNS)
}
```

- [ ] **Step 2: Run tests to verify fail**

Run: `go test ./benchmarking/aws/runner/ -run TestExtractPromPoint -v`
Expected: FAIL — `extractPromPoint` undefined.

- [ ] **Step 3: Implement extractPromPoint**

Append to `prom.go`:

```go
// extractPromPoint pulls the curated metrics out of a single snapshot
// body. Returns ok=false if the snapshot was an error frame.
//
// Hand-rolled rather than depending on prometheus/common/expfmt: the
// curated subset is five unlabeled metrics, and avoiding the dependency
// keeps the runner binary small and the build graph simple.
func extractPromPoint(s promSnapshot) (PromPoint, bool) {
	if s.Errored {
		return PromPoint{}, false
	}
	pp := PromPoint{}
	scanner := bufio.NewScanner(strings.NewReader(s.Body))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, valueStr, ok := splitMetricLine(line)
		if !ok {
			continue
		}
		switch name {
		case "go_goroutines":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.Goroutines = int(n)
		case "go_memstats_heap_inuse_bytes":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.HeapInUseMB = n / 1_000_000 // base-10 to match the rest of the runner
		case "go_memstats_gc_pause_total_ns":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.GCPauseTotalNS = uint64(n)
		case "process_cpu_seconds_total":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.CPUSeconds = n
		case "benchmark_bytes_total":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.BytesTotal = n
		}
	}
	return pp, true
}

// splitMetricLine splits a line like `go_goroutines 312` or
// `go_memstats_heap_inuse_bytes 1.04857e+08`. Labels (curly-brace forms)
// are intentionally NOT supported — the curated subset uses unlabeled
// metrics only.
func splitMetricLine(line string) (name, value string, ok bool) {
	// Skip lines with labels: `metric{label="x"} value`.
	if strings.ContainsRune(line, '{') {
		return "", "", false
	}
	i := strings.IndexRune(line, ' ')
	if i <= 0 || i == len(line)-1 {
		return "", "", false
	}
	return line[:i], strings.TrimSpace(line[i+1:]), true
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `go test ./benchmarking/aws/runner/ -run TestExtractPromPoint -v`
Expected: PASS for all 4 cases.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/prom.go benchmarking/aws/runner/prom_test.go
git commit -m "feat(bench/aws): extractPromPoint — curated metrics → PromPoint"
```

---

## Task 4: Top-level ParsePromStream + T-reindex (TDD)

**Files:**
- Modify: `benchmarking/aws/runner/prom.go`
- Modify: `benchmarking/aws/runner/prom_test.go`

- [ ] **Step 1: Add failing test**

```go
func TestParsePromStream_EndToEnd(t *testing.T) {
	raw := `noise
###timestamp=1000
go_goroutines 10
###timestamp=1010
go_goroutines 12
###timestamp=1020
###scrape_error
###timestamp=1030
go_goroutines 14
`
	pts, err := ParsePromStream(strings.NewReader(raw))
	require.NoError(t, err)
	require.Len(t, pts, 3) // error frame is dropped
	require.Equal(t, 0, pts[0].T)
	require.Equal(t, 10, pts[1].T) // 1010 - 1000
	require.Equal(t, 30, pts[2].T) // 1030 - 1000 (gap kept; the error frame's UnixTime is discarded with its data)
	require.Equal(t, 10, pts[0].Goroutines)
	require.Equal(t, 14, pts[2].Goroutines)
}

func TestParsePromStream_EmptyReader(t *testing.T) {
	pts, err := ParsePromStream(strings.NewReader(""))
	require.NoError(t, err)
	require.Empty(t, pts)
}
```

- [ ] **Step 2: Run test, verify fail**

Run: `go test ./benchmarking/aws/runner/ -run TestParsePromStream -v`
Expected: FAIL — `ParsePromStream` undefined.

- [ ] **Step 3: Implement ParsePromStream**

Append to `prom.go`:

```go
// ParsePromStream consumes a per-point prom dump and returns the curated
// time series with T reindexed to seconds since the first successful
// snapshot.
func ParsePromStream(r io.Reader) ([]PromPoint, error) {
	snaps := parseSnapshots(r)
	var pts []PromPoint
	var t0 int64
	for _, s := range snaps {
		pp, ok := extractPromPoint(s)
		if !ok {
			continue
		}
		if len(pts) == 0 {
			t0 = s.UnixTime
		}
		pp.T = int(s.UnixTime - t0)
		pts = append(pts, pp)
	}
	return pts, nil
}
```

- [ ] **Step 4: Verify pass**

Run: `go test ./benchmarking/aws/runner/ -run TestParsePromStream -v`
Expected: PASS both cases.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/prom.go benchmarking/aws/runner/prom_test.go
git commit -m "feat(bench/aws): ParsePromStream — reader → []PromPoint"
```

---

## Task 5: Add Prom field to PointResult + update render_test

**Files:**
- Modify: `benchmarking/aws/runner/render.go`
- Modify: `benchmarking/aws/runner/render_test.go`

- [ ] **Step 1: Inspect existing PointResult**

Run: `grep -n "type PointResult struct" benchmarking/aws/runner/render.go`

- [ ] **Step 2: Add the field**

Add `Prom []PromPoint` with `omitempty` so older fixtures + tests stay clean. Example:

```go
type PointResult struct {
    VCPU      int        `json:"vcpu"`
    Samples   []Sample   `json:"samples"`
    Summary   Summary    `json:"summary"`
    Anomalies []Anomaly  `json:"anomalies"`
    Prom      []PromPoint `json:"prom,omitempty"` // NEW
}
```

- [ ] **Step 3: Verify existing tests still pass**

Run: `go test ./benchmarking/aws/runner/ -run TestWrite -v`
Expected: PASS. The omitempty makes the JSON round-trip identical for tests with no Prom data.

The render template test (`render_test.go`) may assert the markdown column widths; those are unaffected by this field. If `gofmt -l` flags `render.go` after editing, run `gofmt -w benchmarking/aws/runner/render.go`.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/runner/render.go
git commit -m "feat(bench/aws): PointResult.Prom field (omitempty)"
```

---

## Task 6: Extend bench script with scrape sidecar

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go`

- [ ] **Step 1: Update renderBenchScript**

Find the existing `renderBenchScript` in `matrix.go` (currently ends with `echo "log uploaded"`). Insert the scrape-sidecar block after the heartbeat block and before the `sleep` line. Also add the upload of the prom file after the existing log upload. The full updated function:

```go
func renderBenchScript(a benchScriptArgs) string {
	cpusetHi := 1 + a.VCPU
	totalSec := a.WarmupSec + a.DurationSec
	return strings.Join([]string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting bench: %d vCPU, %d GiB, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`LOG=/tmp/bench-%d.log`, a.VCPU),
		fmt.Sprintf(`PROM=/tmp/prom-%d.txt`, a.VCPU),
		`: > "$LOG"`,
		`: > "$PROM"`,
		fmt.Sprintf(`taskset -c 2-%d chrt --fifo 50 env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s run %s >"$LOG" 2>&1 &`,
			cpusetHi, a.VCPU, a.MemLimitGiB, a.BinaryPath, a.ConfigPath),
		`PID=$!`,
		// Heartbeat (existing)
		`(
  while kill -0 "$PID" 2>/dev/null; do
    sleep 60
    LATEST="$(grep -F 'rolling stats' "$LOG" 2>/dev/null | tail -n 1 || true)"
    if [ -n "$LATEST" ]; then
      echo "[heartbeat] $LATEST"
    else
      echo "[heartbeat] connect running, no samples yet"
    fi
  done
) &`,
		`HEARTBEAT=$!`,
		// Prom scraper (new) — every 10s while Connect is alive, append a
		// framed /metrics snapshot to /tmp/prom-N.txt. ~17min × 6 scrapes/min
		// ≈ 100 frames × ~50KB ≈ 5MB per point. Uploaded post-mortem.
		`(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%s)"
      curl -s --max-time 5 http://localhost:4195/metrics || echo "###scrape_error"
    } >> "$PROM"
    sleep 10
  done
) &`,
		`PROM_SCRAPER=$!`,
		fmt.Sprintf(`sleep %d`, totalSec),
		`kill -TERM "$PID" 2>/dev/null || true`,
		`wait "$PID" 2>/dev/null || true`,
		`kill "$HEARTBEAT" 2>/dev/null || true`,
		`kill "$PROM_SCRAPER" 2>/dev/null || true`,
		`echo "bench point complete"`,
		fmt.Sprintf(`aws s3 cp "$LOG" "s3://%s/runs/%s/sweep-%d.log" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
		fmt.Sprintf(`aws s3 cp "$PROM" "s3://%s/runs/%s/prom-%d.txt" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
		`echo "log uploaded"`,
	}, "\n")
}
```

- [ ] **Step 2: Update the existing script-renders test**

Find `TestRenderBenchScript_EmbedsBucketAndSession` in `matrix_test.go`. Add assertions for the prom-scraper and prom-upload lines. Replace the test function with:

```go
func TestRenderBenchScript_EmbedsBucketAndSession(t *testing.T) {
	got := renderBenchScript(benchScriptArgs{
		VCPU: 4, MemLimitGiB: 4, WarmupSec: 60, DurationSec: 300,
		ConfigPath: "/opt/bench/config.yaml", BinaryPath: "/opt/bench/redpanda-connect",
		Bucket: "my-bucket", SessionID: "sess-1",
	})
	require.Contains(t, got, "/tmp/bench-4.log")
	require.Contains(t, got, "/tmp/prom-4.txt")
	require.Contains(t, got, `s3://my-bucket/runs/sess-1/sweep-4.log`)
	require.Contains(t, got, `s3://my-bucket/runs/sess-1/prom-4.txt`)
	require.Contains(t, got, "taskset -c 2-5")
	require.Contains(t, got, "sleep 360")
	require.Contains(t, got, "GOMEMLIMIT=4GiB")
	require.Contains(t, got, "[heartbeat]")
	require.Contains(t, got, "###timestamp=")
	require.Contains(t, got, "###scrape_error")
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./benchmarking/aws/runner/ -run TestRenderBenchScript -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go
git commit -m "feat(bench/aws): bench script scrapes /metrics + uploads prom dump"
```

---

## Task 7: Wire fetchProm into MatrixRunner.Run (TDD)

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go`
- Modify: `benchmarking/aws/runner/matrix_test.go`

- [ ] **Step 1: Add failing test**

Append to `matrix_test.go`:

```go
func TestMatrixRunner_FetchesPromAlongsideLog(t *testing.T) {
	const sessionID = "bench-test"
	const bucket = "results-bucket"

	logFor := func(vcpu int) string { return makeLog(180, float64(50+vcpu)) }
	prom := `###timestamp=1000
go_goroutines 10
go_memstats_heap_inuse_bytes 1.0485e+08
###timestamp=1010
go_goroutines 12
go_memstats_heap_inuse_bytes 1.1e+08
`
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): logFor(1),
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID):  prom,
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}

	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{
		SSM:            ssm,
		LogFetcher:     fetcher,
		RunnerInstance: "i-runner",
		Bucket:         bucket,
		SessionID:      sessionID,
	}
	points, err := mr.Run(context.Background(), []int{1}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Len(t, points[0].Prom, 2)
	require.Equal(t, 10, points[0].Prom[0].Goroutines)
	require.Equal(t, 0, points[0].Prom[0].T)
	require.Equal(t, 10, points[0].Prom[1].T)
}

func TestMatrixRunner_MissingPromIsNonFatal(t *testing.T) {
	const sessionID = "bench-test"
	logFor := func(vcpu int) string { return makeLog(180, 50) }
	fetcher := &FakeLogFetcher{
		Contents: map[string]string{
			fmt.Sprintf("runs/%s/sweep-1.log", sessionID): logFor(1),
			// no prom-1.txt
		},
		Errs: map[string]error{
			fmt.Sprintf("runs/%s/prom-1.txt", sessionID): fmt.Errorf("not found"),
		},
	}
	ssm := &FakeSSM{Transcripts: map[string][]string{"i-runner": nil}}
	prev := stdout
	stdout = &bytes.Buffer{}
	defer func() { stdout = prev }()

	mr := &MatrixRunner{SSM: ssm, LogFetcher: fetcher, RunnerInstance: "i-runner", Bucket: "b", SessionID: sessionID}
	points, err := mr.Run(context.Background(), []int{1}, 1, 60*time.Second, 120*time.Second, "", "")
	require.NoError(t, err, "missing prom dump must not fail the sweep point")
	require.Len(t, points, 1)
	require.Empty(t, points[0].Prom, "Prom stays nil/empty when fetch failed")
}
```

- [ ] **Step 2: Run test to verify fail**

Run: `go test ./benchmarking/aws/runner/ -run TestMatrixRunner_FetchesPromAlongsideLog -v`
Expected: FAIL — `points[0].Prom` empty (zero length) because we haven't wired fetchProm yet. (The second test will pass already since Prom defaults to nil.)

- [ ] **Step 3: Add fetchProm helper and call it in Run**

Find the existing `fetchLog` method in `matrix.go`. Add directly below it:

```go
// fetchProm downloads the per-point Prometheus dump uploaded by the bench
// script. Failure is non-fatal — the sweep point is still useful without
// goroutine/heap context.
func (m *MatrixRunner) fetchProm(ctx context.Context, vcpu int) []PromPoint {
	if m.LogFetcher == nil {
		return nil
	}
	key := fmt.Sprintf("runs/%s/prom-%d.txt", m.SessionID, vcpu)
	body, err := m.LogFetcher.Fetch(ctx, m.Bucket, key)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] fetch prom (non-fatal): %v\n", err)
		return nil
	}
	defer body.Close()
	pts, err := ParsePromStream(body)
	if err != nil {
		fmt.Fprintf(stdout, "[bench] parse prom (non-fatal): %v\n", err)
		return nil
	}
	return pts
}
```

In `Run`, after the existing `samples := parseAndTrim(raw, warmup)` line, add:

```go
promPts := m.fetchProm(ctx, n)
```

Then in the `out = append(out, SweepPoint{…})` block, add the `Prom` field. Find:

```go
out = append(out, SweepPoint{
    VCPU:      n,
    Samples:   samples,
    Summary:   summary,
    Anomalies: anomalies,
})
```

Replace with:

```go
out = append(out, SweepPoint{
    VCPU:      n,
    Samples:   samples,
    Summary:   summary,
    Anomalies: anomalies,
    Prom:      promPts,
})
```

- [ ] **Step 4: Add Prom field to SweepPoint**

Find `type SweepPoint struct` in `matrix.go`. Add `Prom []PromPoint`:

```go
type SweepPoint struct {
	VCPU      int
	Samples   []Sample
	Summary   Summary
	Anomalies []Anomaly
	Prom      []PromPoint
}
```

- [ ] **Step 5: Propagate Prom into PointResult at the runBench glue**

In `main.go`, find the `result.Points = append(result.Points, PointResult{…})` block in `runBench`. Add the `Prom` field:

```go
result.Points = append(result.Points, PointResult{
    VCPU:      p.VCPU,
    Samples:   p.Samples,
    Summary:   p.Summary,
    Anomalies: p.Anomalies,
    Prom:      p.Prom,
})
```

- [ ] **Step 6: Run all runner tests**

Run: `go test ./benchmarking/aws/runner/ -v 2>&1 | tail -50`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): MatrixRunner fetches + embeds Prom time series"
```

---

## Task 8: Anomaly prom-context fields + populator (TDD)

**Files:**
- Modify: `benchmarking/aws/runner/anomalies.go`
- Modify: `benchmarking/aws/runner/anomalies_test.go`

- [ ] **Step 1: Inspect existing Anomaly struct + DetectAnomalies signature**

Run: `grep -n "type Anomaly\|func DetectAnomalies" benchmarking/aws/runner/anomalies.go`

- [ ] **Step 2: Add failing test**

Append to `anomalies_test.go`:

```go
func TestDetectAnomaliesWithProm_AttachesContext(t *testing.T) {
	// Throughput dip from T=60..130s; samples below threshold.
	samples := []Sample{}
	for i := 0; i < 200; i++ {
		mb := 100.0
		if i >= 60 && i < 130 {
			mb = 50.0 // dip
		}
		samples = append(samples, Sample{T: i, MBPerSec: mb})
	}
	// Prom snapshots every 10s starting at T=0.
	prom := []PromPoint{
		{T: 0, Goroutines: 100, HeapInUseMB: 50, GCPauseTotalNS: 100_000},
		{T: 10, Goroutines: 100, HeapInUseMB: 51, GCPauseTotalNS: 110_000},
		{T: 20, Goroutines: 100, HeapInUseMB: 51, GCPauseTotalNS: 120_000},
		{T: 30, Goroutines: 100, HeapInUseMB: 52, GCPauseTotalNS: 130_000},
		{T: 40, Goroutines: 100, HeapInUseMB: 52, GCPauseTotalNS: 140_000},
		{T: 50, Goroutines: 100, HeapInUseMB: 52, GCPauseTotalNS: 150_000},
		{T: 60, Goroutines: 500, HeapInUseMB: 1400, GCPauseTotalNS: 1_000_000_000}, // spike at dip start
		{T: 70, Goroutines: 510, HeapInUseMB: 1410, GCPauseTotalNS: 1_500_000_000},
	}
	anomalies := DetectAnomaliesWithProm(samples, 100.0, prom)
	require.Len(t, anomalies, 1)
	a := anomalies[0]
	require.Equal(t, 60, a.StartT)
	require.Equal(t, 500, a.GoroutinesAtStart)
	require.InDelta(t, 1400.0, a.HeapInUseMBAtStart, 0.1)
	// GCPauseDeltaNS = pause_at_T60 - pause_at_T50 = 1_000_000_000 - 150_000 = 999_850_000
	require.Equal(t, uint64(999_850_000), a.GCPauseDeltaNS)
}

func TestDetectAnomaliesWithProm_NoPromKeepsZeros(t *testing.T) {
	samples := []Sample{}
	for i := 0; i < 200; i++ {
		mb := 100.0
		if i >= 60 && i < 130 {
			mb = 50.0
		}
		samples = append(samples, Sample{T: i, MBPerSec: mb})
	}
	anomalies := DetectAnomaliesWithProm(samples, 100.0, nil)
	require.Len(t, anomalies, 1)
	require.Equal(t, 0, anomalies[0].GoroutinesAtStart)
	require.Equal(t, uint64(0), anomalies[0].GCPauseDeltaNS)
}
```

- [ ] **Step 3: Run test, verify fail**

Run: `go test ./benchmarking/aws/runner/ -run TestDetectAnomaliesWithProm -v`
Expected: FAIL — `DetectAnomaliesWithProm` undefined; the new Anomaly fields don't exist.

- [ ] **Step 4: Extend Anomaly struct**

In `anomalies.go`, add to the `Anomaly` struct (existing fields kept):

```go
type Anomaly struct {
    StartT      int     `json:"start_t"`
    EndT        int     `json:"end_t"`
    DurationSec int     `json:"duration_sec"`
    MinMBPerSec float64 `json:"min_mb_per_sec"`
    Note        string  `json:"note"`

    // Prom context (populated when prom data is available; zero otherwise).
    GoroutinesAtStart  int     `json:"goroutines_at_start,omitempty"`
    HeapInUseMBAtStart float64 `json:"heap_in_use_mb_at_start,omitempty"`
    GCPauseDeltaNS     uint64  `json:"gc_pause_delta_ns,omitempty"`
}
```

(Match the existing field names — adjust if your local Anomaly uses different `StartT`/`EndT` names. Keep the rest of the struct identical.)

- [ ] **Step 5: Implement DetectAnomaliesWithProm**

Append to `anomalies.go`:

```go
// DetectAnomaliesWithProm wraps DetectAnomalies and annotates each anomaly
// with the prom-curated context at the anomaly's start, plus the GC pause
// delta over the 10s window preceding the start. When prom is empty, the
// returned anomalies match DetectAnomalies exactly.
func DetectAnomaliesWithProm(samples []Sample, reference float64, prom []PromPoint) []Anomaly {
	base := DetectAnomalies(samples, reference)
	if len(prom) == 0 {
		return base
	}
	for i := range base {
		atStart, ok := promNearestAtOrBefore(prom, base[i].StartT)
		if !ok {
			continue
		}
		base[i].GoroutinesAtStart = atStart.Goroutines
		base[i].HeapInUseMBAtStart = atStart.HeapInUseMB
		// GC pause delta: pause-counter at the snapshot AT/BEFORE the start
		// minus the one 10s prior. Yields ns of GC pause during that interval.
		prev, hadPrev := promNearestAtOrBefore(prom, base[i].StartT-10)
		if hadPrev && atStart.GCPauseTotalNS >= prev.GCPauseTotalNS {
			base[i].GCPauseDeltaNS = atStart.GCPauseTotalNS - prev.GCPauseTotalNS
		}
	}
	return base
}

func promNearestAtOrBefore(prom []PromPoint, t int) (PromPoint, bool) {
	var best PromPoint
	found := false
	for _, p := range prom {
		if p.T <= t {
			best = p
			found = true
		} else {
			break // prom is in T order
		}
	}
	return best, found
}
```

- [ ] **Step 6: Wire DetectAnomaliesWithProm into MatrixRunner.Run**

In `matrix.go`, find:

```go
summary := Summarise(samples)
anomalies := DetectAnomalies(samples, summary.MedianMBPerSec)
```

Replace the `DetectAnomalies` line with:

```go
anomalies := DetectAnomaliesWithProm(samples, summary.MedianMBPerSec, promPts)
```

(Move `promPts := m.fetchProm(ctx, n)` up above this block if it isn't already.)

- [ ] **Step 7: Run tests**

Run: `go test ./benchmarking/aws/runner/ -v 2>&1 | tail -60`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add benchmarking/aws/runner/anomalies.go benchmarking/aws/runner/anomalies_test.go benchmarking/aws/runner/matrix.go
git commit -m "feat(bench/aws): annotate anomalies with prom-curated context"
```

---

## Task 9: Smoke test the full chain (no AWS)

**Files:** none

- [ ] **Step 1: Build the runner**

Run: `go build ./benchmarking/aws/runner/`
Expected: clean.

- [ ] **Step 2: Run all unit tests + race**

Run: `go test -race ./benchmarking/aws/runner/...`
Expected: PASS, no races.

- [ ] **Step 3: Cross-compile for arm64 (matches what gets shipped)**

Run: `GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/runner-arm64 ./benchmarking/aws/runner/ && rm /tmp/runner-arm64`
Expected: silent success.

- [ ] **Step 4: gofmt check**

Run: `gofmt -l benchmarking/aws/runner/prom.go benchmarking/aws/runner/prom_test.go benchmarking/aws/runner/matrix.go benchmarking/aws/runner/anomalies.go`
Expected: empty output.

---

## Verification checklist

- [ ] All `go test ./benchmarking/aws/runner/...` tests pass
- [ ] `go vet` clean
- [ ] `gofmt -l` clean on all modified files
- [ ] (Operator) A real bench run produces:
  - `runs/<session>/prom-N.txt` in S3 for each sweep point
  - `points[*].prom` populated (length ~100 per point) in the result JSON
  - `anomalies[*].goroutines_at_start` and `heap_in_use_mb_at_start` populated when anomalies fire

## Done criteria

A bench run captures Connect's runtime metrics every 10s per sweep point. The result JSON includes the time series. Detected throughput dips carry the goroutine/heap/GC-pause context at the moment they started. Future bench retries can diagnose anomalies offline without re-running the bench.
