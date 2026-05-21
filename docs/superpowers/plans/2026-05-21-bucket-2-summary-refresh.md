# Bucket 2 — SUMMARY.md auto-refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After every `task aws:bench`, the marker-delimited "AWS Bench Results" section in `docs/benchmark-results/SUMMARY.md` is regenerated from the latest JSON per `(connector, scenario)` pair under `benchmarking/aws/results/`. Also expose a `runner summary` CLI for offline regen.

**Architecture:** A new `summary.go` file in the runner package walks `results/`, picks the latest per scenario, derives one table row each, and writes the result into a `<!-- bench:aws:start --> … <!-- bench:aws:end -->` region of `SUMMARY.md` using a `go:embed`-included template. Atomic write via `os.Rename`. `runBench` calls `RefreshSummary` at the end (right after `AppendMarkdown`); a new `runner summary` subcommand exposes the same call for offline regen.

**Tech Stack:** Go stdlib only (`path/filepath`, `os`, `text/template`, `embed`), `testify` for tests.

**Spec:** [`docs/superpowers/specs/2026-05-21-bucket-2-foundation-polish-design.md`](../specs/2026-05-21-bucket-2-foundation-polish-design.md) — Item 4.

**Out of scope:** Backfilling the existing hand-written rows in SUMMARY.md (Migrator, DynamoDB CDC, etc.) — those come from `internal/impl/*/bench/` and stay manually curated. Multi-scenario aggregation (best-of, comparison tables).

---

## File Structure

```
benchmarking/aws/runner/
├── summary.go                              # NEW — RefreshSummary, walkResults, marker-bounded write
├── summary_test.go                         # NEW — t.TempDir() fixtures, byte-exact assertions
├── templates/
│   └── summary-section.md.tmpl             # NEW — embedded via go:embed
└── main.go                                 # MODIFY — `summary` subcommand + RefreshSummary call at end of runBench

benchmarking/aws/Taskfile.yml               # MODIFY — add aws:summary task
docs/benchmark-results/SUMMARY.md           # MODIFY — insert markers (one-time)
```

## Conventions

- License header on every new `.go` and `.md.tmpl` file (BSL block; for the template use the HTML comment form: `<!-- Copyright 2025 Redpanda Data, Inc. -->`).
- `go:embed` pattern mirrors the existing one in `render.go` for `templates/result.md.tmpl`.
- Tests use `t.TempDir()` and write JSON fixtures inline; no global fixtures.

---

## Task 1: One-time markers in SUMMARY.md

**Files:**
- Modify: `docs/benchmark-results/SUMMARY.md`

- [ ] **Step 1: Insert markers**

Open `docs/benchmark-results/SUMMARY.md`. Find a place to anchor the auto-section (recommendation: right before the existing `## Test Conditions` section, so AWS results appear after the at-a-glance laptop summary but before the conditions caveat).

Insert exactly these two lines (with a blank line above and below for readability), no content between them yet:

```markdown

<!-- bench:aws:start - auto-generated, do not edit by hand -->
<!-- bench:aws:end -->

```

- [ ] **Step 2: Verify markers are present and unique**

Run: `grep -c "bench:aws:start" docs/benchmark-results/SUMMARY.md`
Expected: `1`.

Run: `grep -c "bench:aws:end" docs/benchmark-results/SUMMARY.md`
Expected: `1`.

- [ ] **Step 3: Commit**

```bash
git add docs/benchmark-results/SUMMARY.md
git commit -m "docs(bench/aws): insert markers for auto-generated AWS section"
```

---

## Task 2: TDD — walkResults discovers latest JSON per scenario

**Files:**
- Create: `benchmarking/aws/runner/summary.go`
- Create: `benchmarking/aws/runner/summary_test.go`

- [ ] **Step 1: Create summary.go skeleton**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"os"
	"path/filepath"
	"sort"
)

// summaryRow is one line in the auto-rendered table.
type summaryRow struct {
	ConnectorScenario string  // "postgres / orders-cdc"
	PeakMBPerSec      float64 // 0 if every point was zero
	BestVCPU          int     // 0 sentinel when peak is 0
	MedianAtBestMB    float64
	LastRunDate       string  // YYYY-MM-DD
	ResultJSONPath    string  // relative to repo root, for footnote linking
}
```

- [ ] **Step 2: Create summary_test.go with failing test**

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

// writeResult is a test helper that writes a Result JSON to the standard
// per-scenario subdirectory layout.
func writeResult(t *testing.T, root, connector, scenario, timestamp string, r *Result) string {
	t.Helper()
	dir := filepath.Join(root, connector, scenario)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, timestamp+".json")
	b, err := json.Marshal(r)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, b, 0o644))
	return path
}

func TestWalkResults_EmptyDir(t *testing.T) {
	root := t.TempDir()
	rows, err := walkResults(root)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestWalkResults_OneScenarioOneFile(t *testing.T) {
	root := t.TempDir()
	r := &Result{
		Scenario:   "postgres/orders-cdc",
		FinishedAt: time.Date(2026, 5, 21, 1, 35, 18, 0, time.UTC),
		Points: []PointResult{
			{VCPU: 1, Summary: Summary{MedianMBPerSec: 76, PeakMBPerSec: 83}},
			{VCPU: 4, Summary: Summary{MedianMBPerSec: 99, PeakMBPerSec: 102}},
		},
	}
	writeResult(t, root, "postgres", "orders-cdc", "2026-05-21T01-35-18Z", r)

	rows, err := walkResults(root)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "postgres / orders-cdc", rows[0].ConnectorScenario)
	require.Equal(t, 102.0, rows[0].PeakMBPerSec)
	require.Equal(t, 4, rows[0].BestVCPU)
	require.Equal(t, 99.0, rows[0].MedianAtBestMB)
	require.Equal(t, "2026-05-21", rows[0].LastRunDate)
}

func TestWalkResults_PicksNewestPerScenario(t *testing.T) {
	root := t.TempDir()
	older := &Result{
		Scenario:   "postgres/orders-cdc",
		FinishedAt: time.Date(2026, 5, 20, 4, 46, 38, 0, time.UTC),
		Points:     []PointResult{{VCPU: 1, Summary: Summary{MedianMBPerSec: 5, PeakMBPerSec: 6}}},
	}
	newer := &Result{
		Scenario:   "postgres/orders-cdc",
		FinishedAt: time.Date(2026, 5, 21, 1, 35, 18, 0, time.UTC),
		Points:     []PointResult{{VCPU: 1, Summary: Summary{MedianMBPerSec: 76, PeakMBPerSec: 83}}},
	}
	writeResult(t, root, "postgres", "orders-cdc", "2026-05-20T04-46-38Z", older)
	writeResult(t, root, "postgres", "orders-cdc", "2026-05-21T01-35-18Z", newer)

	rows, err := walkResults(root)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, 83.0, rows[0].PeakMBPerSec)
}

func TestWalkResults_MultiScenarioAlphabetical(t *testing.T) {
	root := t.TempDir()
	writeResult(t, root, "postgres", "orders-cdc", "2026-05-21T00-00-00Z", &Result{
		Scenario: "postgres/orders-cdc", FinishedAt: time.Now().UTC(),
		Points: []PointResult{{VCPU: 1, Summary: Summary{PeakMBPerSec: 1}}},
	})
	writeResult(t, root, "mysql", "orders-cdc", "2026-05-21T00-00-00Z", &Result{
		Scenario: "mysql/orders-cdc", FinishedAt: time.Now().UTC(),
		Points: []PointResult{{VCPU: 1, Summary: Summary{PeakMBPerSec: 1}}},
	})
	rows, err := walkResults(root)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "mysql / orders-cdc", rows[0].ConnectorScenario)
	require.Equal(t, "postgres / orders-cdc", rows[1].ConnectorScenario)
}

func TestWalkResults_AllZeroPeaks(t *testing.T) {
	root := t.TempDir()
	writeResult(t, root, "postgres", "broken", "2026-05-21T00-00-00Z", &Result{
		Scenario: "postgres/broken", FinishedAt: time.Now().UTC(),
		Points: []PointResult{
			{VCPU: 1, Summary: Summary{PeakMBPerSec: 0}},
			{VCPU: 2, Summary: Summary{PeakMBPerSec: 0}},
		},
	})
	rows, err := walkResults(root)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, 0.0, rows[0].PeakMBPerSec)
	require.Equal(t, 0, rows[0].BestVCPU) // 0 sentinel
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./benchmarking/aws/runner/ -run TestWalkResults -v`
Expected: FAIL — `walkResults` undefined.

- [ ] **Step 4: Implement walkResults in summary.go**

Append to `summary.go`:

```go
import (
	"encoding/json"
	"fmt"
)

// walkResults discovers every <root>/<connector>/<scenario>/*.json result
// file, picks the newest per (connector, scenario), and derives one
// summaryRow per scenario sorted alphabetically by ConnectorScenario.
func walkResults(root string) ([]summaryRow, error) {
	connectors, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read results root %s: %w", root, err)
	}
	var rows []summaryRow
	for _, c := range connectors {
		if !c.IsDir() {
			continue
		}
		connDir := filepath.Join(root, c.Name())
		scenarios, err := os.ReadDir(connDir)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", connDir, err)
		}
		for _, s := range scenarios {
			if !s.IsDir() {
				continue
			}
			scenDir := filepath.Join(connDir, s.Name())
			jsons, err := filepath.Glob(filepath.Join(scenDir, "*.json"))
			if err != nil {
				return nil, fmt.Errorf("glob %s: %w", scenDir, err)
			}
			if len(jsons) == 0 {
				continue
			}
			sort.Strings(jsons) // timestamp prefix → lexicographic == chronological
			latest := jsons[len(jsons)-1]
			row, err := derivedRow(c.Name(), s.Name(), latest)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ConnectorScenario < rows[j].ConnectorScenario })
	return rows, nil
}

// derivedRow loads one Result JSON and returns the summary row for it.
func derivedRow(connector, scenario, jsonPath string) (summaryRow, error) {
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		return summaryRow{}, fmt.Errorf("read %s: %w", jsonPath, err)
	}
	var r Result
	if err := json.Unmarshal(raw, &r); err != nil {
		return summaryRow{}, fmt.Errorf("parse %s: %w", jsonPath, err)
	}
	row := summaryRow{
		ConnectorScenario: connector + " / " + scenario,
		LastRunDate:       r.FinishedAt.UTC().Format("2006-01-02"),
		ResultJSONPath:    jsonPath,
	}
	for _, p := range r.Points {
		if p.Summary.PeakMBPerSec > row.PeakMBPerSec {
			row.PeakMBPerSec = p.Summary.PeakMBPerSec
			row.BestVCPU = p.VCPU
			row.MedianAtBestMB = p.Summary.MedianMBPerSec
		}
	}
	return row, nil
}
```

- [ ] **Step 5: Run tests to verify pass**

Run: `go test ./benchmarking/aws/runner/ -run TestWalkResults -v`
Expected: PASS for all 5 cases.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/summary.go benchmarking/aws/runner/summary_test.go
git commit -m "feat(bench/aws): walkResults — latest JSON per scenario → summaryRow"
```

---

## Task 3: TDD — Render the table template

**Files:**
- Create: `benchmarking/aws/runner/templates/summary-section.md.tmpl`
- Modify: `benchmarking/aws/runner/summary.go`
- Modify: `benchmarking/aws/runner/summary_test.go`

- [ ] **Step 1: Create the embedded template**

```
<!-- Copyright 2025 Redpanda Data, Inc. -->
## AWS Bench Results

Last refreshed: {{.LastRefreshed}}

| Connector / Scenario  | Peak MB/s | At vCPU | Median (best vCPU) | Last Run    |
|-----------------------|-----------|---------|--------------------|-------------|
{{- if not .Rows }}
| *(no AWS runs yet)*   |        —  |       — |                 — | —           |
{{- end }}
{{- range .Rows }}
| {{ printf "%-21s" .ConnectorScenario }} | {{ if eq .PeakMBPerSec 0.0 }}     —    {{ else }}{{ printf "%9.0f" .PeakMBPerSec }}{{ end }} | {{ if eq .BestVCPU 0 }}    —  {{ else }}{{ printf "%7d" .BestVCPU }}{{ end }} | {{ if eq .PeakMBPerSec 0.0 }}              —    {{ else }}{{ printf "%18.0f" .MedianAtBestMB }}{{ end }} | {{ printf "%-11s" .LastRunDate }} |
{{- end }}

Each row is the **latest** run of that scenario. Raw samples + Prometheus snapshots live under `results/<connector>/<scenario>/`.

To regenerate without running a bench: `task aws:summary`
```

- [ ] **Step 2: Add failing render test**

Append to `summary_test.go`:

```go
import (
	"bytes"
	// ... existing
)

func TestRenderSection_OneRow(t *testing.T) {
	rows := []summaryRow{
		{ConnectorScenario: "postgres / orders-cdc", PeakMBPerSec: 102, BestVCPU: 4, MedianAtBestMB: 99, LastRunDate: "2026-05-21"},
	}
	var buf bytes.Buffer
	require.NoError(t, renderSection(&buf, rows, "2026-05-21"))
	out := buf.String()
	require.Contains(t, out, "## AWS Bench Results")
	require.Contains(t, out, "Last refreshed: 2026-05-21")
	require.Contains(t, out, "postgres / orders-cdc")
	require.Contains(t, out, "102")
	require.Contains(t, out, "2026-05-21")
}

func TestRenderSection_EmptyRows(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, renderSection(&buf, nil, "2026-05-21"))
	require.Contains(t, buf.String(), "*(no AWS runs yet)*")
}

func TestRenderSection_ZeroPeakShowsDash(t *testing.T) {
	rows := []summaryRow{
		{ConnectorScenario: "postgres / broken", PeakMBPerSec: 0, BestVCPU: 0, LastRunDate: "2026-05-21"},
	}
	var buf bytes.Buffer
	require.NoError(t, renderSection(&buf, rows, "2026-05-21"))
	out := buf.String()
	require.Contains(t, out, "postgres / broken")
	require.Contains(t, out, "—")
}
```

- [ ] **Step 3: Run test to verify fail**

Run: `go test ./benchmarking/aws/runner/ -run TestRenderSection -v`
Expected: FAIL — `renderSection` undefined.

- [ ] **Step 4: Implement renderSection in summary.go**

Append to `summary.go`:

```go
import (
	_ "embed"
	"io"
	"text/template"
)

//go:embed templates/summary-section.md.tmpl
var summaryTmplSrc string

var summaryTmpl = template.Must(template.New("summary").Parse(summaryTmplSrc))

// renderSection writes the rendered table to w. The leading comment marker
// is NOT emitted here — RefreshSummary owns the markers because it has to
// match them exactly against existing file content.
func renderSection(w io.Writer, rows []summaryRow, lastRefreshed string) error {
	return summaryTmpl.Execute(w, struct {
		Rows          []summaryRow
		LastRefreshed string
	}{Rows: rows, LastRefreshed: lastRefreshed})
}
```

- [ ] **Step 5: Run tests to verify pass**

Run: `go test ./benchmarking/aws/runner/ -run TestRenderSection -v`
Expected: PASS for all 3 cases.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/summary.go benchmarking/aws/runner/summary_test.go benchmarking/aws/runner/templates/summary-section.md.tmpl
git commit -m "feat(bench/aws): renderSection — embedded summary template"
```

---

## Task 4: TDD — RefreshSummary marker-bounded write

**Files:**
- Modify: `benchmarking/aws/runner/summary.go`
- Modify: `benchmarking/aws/runner/summary_test.go`

- [ ] **Step 1: Add failing tests**

Append to `summary_test.go`:

```go
import (
	"strings"
	// ... existing
)

const markerStart = "<!-- bench:aws:start - auto-generated, do not edit by hand -->"
const markerEnd = "<!-- bench:aws:end -->"

func writeSummaryFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "SUMMARY.md")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))
	return path
}

func TestRefreshSummary_PreservesContentOutsideMarkers(t *testing.T) {
	resultsRoot := t.TempDir()
	writeResult(t, resultsRoot, "postgres", "orders-cdc", "2026-05-21T00-00-00Z", &Result{
		Scenario:   "postgres/orders-cdc",
		FinishedAt: time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC),
		Points:     []PointResult{{VCPU: 4, Summary: Summary{PeakMBPerSec: 102, MedianMBPerSec: 99}}},
	})

	above := "# Some Doc\n\nFirst paragraph.\n\n"
	below := "\nSecond paragraph after the section.\n"
	contents := above + markerStart + "\n" + markerEnd + below
	summaryPath := writeSummaryFile(t, contents)

	require.NoError(t, RefreshSummary(summaryPath, resultsRoot, time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)))

	raw, err := os.ReadFile(summaryPath)
	require.NoError(t, err)
	out := string(raw)
	require.True(t, strings.HasPrefix(out, above), "preserve content above markers")
	require.True(t, strings.HasSuffix(out, below), "preserve content below markers")
	require.Contains(t, out, markerStart)
	require.Contains(t, out, markerEnd)
	require.Contains(t, out, "## AWS Bench Results")
	require.Contains(t, out, "postgres / orders-cdc")
}

func TestRefreshSummary_MissingMarkersAppends(t *testing.T) {
	resultsRoot := t.TempDir()
	contents := "# Some Doc\n\nNo markers here.\n"
	summaryPath := writeSummaryFile(t, contents)

	require.NoError(t, RefreshSummary(summaryPath, resultsRoot, time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)))

	raw, err := os.ReadFile(summaryPath)
	require.NoError(t, err)
	out := string(raw)
	require.True(t, strings.HasPrefix(out, contents), "preserve original content")
	require.Contains(t, out, markerStart)
	require.Contains(t, out, markerEnd)
}

func TestRefreshSummary_AtomicTmpRename(t *testing.T) {
	// Make sure no SUMMARY.md.tmp lingers after a successful refresh.
	resultsRoot := t.TempDir()
	contents := markerStart + "\n" + markerEnd
	summaryPath := writeSummaryFile(t, contents)
	require.NoError(t, RefreshSummary(summaryPath, resultsRoot, time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)))

	_, err := os.Stat(summaryPath + ".tmp")
	require.True(t, os.IsNotExist(err), "tmp file should be gone after rename")
}
```

- [ ] **Step 2: Run tests to verify fail**

Run: `go test ./benchmarking/aws/runner/ -run TestRefreshSummary -v`
Expected: FAIL — `RefreshSummary` undefined.

- [ ] **Step 3: Implement RefreshSummary**

Append to `summary.go`:

```go
import (
	"bytes"
	"strings"
	"time"
)

const (
	SummaryMarkerStart = "<!-- bench:aws:start - auto-generated, do not edit by hand -->"
	SummaryMarkerEnd   = "<!-- bench:aws:end -->"
)

// RefreshSummary walks resultsRoot, derives the latest-per-scenario rows,
// renders the section, and writes it between the markers in summaryPath.
// If markers are missing, the section is appended to the end of the file
// and a one-line warning is written to os.Stderr.
//
// The write is atomic: tmp file + rename, mirroring WriteResultJSON.
func RefreshSummary(summaryPath, resultsRoot string, now time.Time) error {
	rows, err := walkResults(resultsRoot)
	if err != nil {
		return fmt.Errorf("walk results: %w", err)
	}

	var section bytes.Buffer
	section.WriteString(SummaryMarkerStart)
	section.WriteByte('\n')
	if err := renderSection(&section, rows, now.UTC().Format("2006-01-02")); err != nil {
		return fmt.Errorf("render section: %w", err)
	}
	section.WriteByte('\n')
	section.WriteString(SummaryMarkerEnd)

	existing, err := os.ReadFile(summaryPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", summaryPath, err)
	}

	var next []byte
	startIdx := bytes.Index(existing, []byte(SummaryMarkerStart))
	endIdx := bytes.Index(existing, []byte(SummaryMarkerEnd))
	switch {
	case startIdx >= 0 && endIdx > startIdx:
		// Replace from start of start-marker to end of end-marker.
		endTotal := endIdx + len(SummaryMarkerEnd)
		next = append(next, existing[:startIdx]...)
		next = append(next, section.Bytes()...)
		next = append(next, existing[endTotal:]...)
	default:
		// Markers missing: append a fresh section to the end with a leading blank line.
		fmt.Fprintln(os.Stderr, "warning: bench:aws markers not found in "+summaryPath+"; appending section to end of file")
		next = append(next, existing...)
		if !strings.HasSuffix(string(existing), "\n") {
			next = append(next, '\n')
		}
		next = append(next, '\n')
		next = append(next, section.Bytes()...)
		next = append(next, '\n')
	}

	tmp := summaryPath + ".tmp"
	if err := os.WriteFile(tmp, next, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, summaryPath); err != nil {
		return fmt.Errorf("rename tmp: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `go test ./benchmarking/aws/runner/ -run TestRefreshSummary -v`
Expected: PASS for all 3 cases.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/summary.go benchmarking/aws/runner/summary_test.go
git commit -m "feat(bench/aws): RefreshSummary — marker-bounded atomic write"
```

---

## Task 5: Wire RefreshSummary into runBench

**Files:**
- Modify: `benchmarking/aws/runner/main.go`

- [ ] **Step 1: Locate the existing markdown append at end of runBench**

Find the block in `main.go` (around L267):

```go
mdPath := filepath.Join(opts.repoRoot, "docs/benchmark-results", s.Stack+".md")
if err := AppendMarkdown(mdPath, result, strings.TrimSpace(s.Description)); err != nil {
    return err
}
fmt.Printf("\n✓ done — JSON: %s\n           md: %s\n", jsonPath, mdPath)
return nil
```

- [ ] **Step 2: Insert RefreshSummary call before the final Printf**

Replace the block above with:

```go
mdPath := filepath.Join(opts.repoRoot, "docs/benchmark-results", s.Stack+".md")
if err := AppendMarkdown(mdPath, result, strings.TrimSpace(s.Description)); err != nil {
    return err
}
summaryPath := filepath.Join(opts.repoRoot, "docs/benchmark-results/SUMMARY.md")
if err := RefreshSummary(summaryPath, resultsDir, time.Now()); err != nil {
    // Non-fatal: a bench run that produced a valid result should not fail
    // because the project-level summary couldn't be rewritten.
    fmt.Fprintf(os.Stderr, "warning: refresh SUMMARY.md: %v\n", err)
}
fmt.Printf("\n✓ done — JSON: %s\n           md: %s\n           summary: %s\n", jsonPath, mdPath, summaryPath)
return nil
```

- [ ] **Step 3: Build**

Run: `go build ./benchmarking/aws/runner/`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): runBench refreshes SUMMARY.md after AppendMarkdown"
```

---

## Task 6: Add `summary` subcommand + Taskfile

**Files:**
- Modify: `benchmarking/aws/runner/main.go`
- Modify: `benchmarking/aws/Taskfile.yml`

- [ ] **Step 1: Add summaryCmd function**

Append to `main.go`:

```go
func summaryCmd(args []string) error {
	fs := flag.NewFlagSet("summary", flag.ExitOnError)
	repoRoot := fs.String("repo-root", ".", "path to the connect repo root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	summaryPath := filepath.Join(*repoRoot, "docs/benchmark-results/SUMMARY.md")
	resultsDir := filepath.Join(*repoRoot, "benchmarking/aws/results")
	if err := RefreshSummary(summaryPath, resultsDir, time.Now()); err != nil {
		return err
	}
	fmt.Printf("refreshed %s\n", summaryPath)
	return nil
}
```

- [ ] **Step 2: Register in main switch**

Find the existing switch in `main()`:

```go
switch os.Args[1] {
case "bench":
    exitOnErr(benchCmd(os.Args[2:]))
case "validate":
    exitOnErr(validateCmd(os.Args[2:]))
case "down":
    exitOnErr(downCmd(os.Args[2:]))
case "cost-check":
    exitOnErr(costCheckCmd(os.Args[2:]))
default:
    usage()
    os.Exit(2)
}
```

Add the `summary` case before `default`:

```go
case "summary":
    exitOnErr(summaryCmd(os.Args[2:]))
```

Update `usage()` to mention it:

```go
func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  runner bench --scenario=<path> [--keep] [--keep-on-fail]
  runner validate --scenario=<path>
  runner down --scenario=<path>
  runner cost-check
  runner summary [--repo-root=<path>]`)
}
```

- [ ] **Step 3: Add Taskfile entry**

Append (or insert near the other `aws:*` entries) in `benchmarking/aws/Taskfile.yml`:

```yaml
  summary:
    desc: "Regenerate the auto-managed AWS section in docs/benchmark-results/SUMMARY.md"
    cmds:
      - cd ../.. && go run ./benchmarking/aws/runner summary --repo-root=.
```

- [ ] **Step 4: Smoke test**

Run from the repo root: `go run ./benchmarking/aws/runner summary --repo-root=.`
Expected: `refreshed /Users/.../docs/benchmark-results/SUMMARY.md`.

Run: `grep -A2 'bench:aws:start' docs/benchmark-results/SUMMARY.md | head -5`
Expected: shows `## AWS Bench Results` after the marker line.

- [ ] **Step 5: Build and vet**

Run: `go build ./benchmarking/aws/runner/ && go vet ./benchmarking/aws/runner/`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/main.go benchmarking/aws/Taskfile.yml
git commit -m "feat(bench/aws): summary subcommand + task aws:summary"
```

---

## Task 7: Document in README

**Files:**
- Modify: `benchmarking/aws/README.md`

- [ ] **Step 1: Add or update the task table**

Add a row (or paragraph) for `task aws:summary`:

```markdown
| `task aws:summary` | Regenerate the auto-managed AWS section in `docs/benchmark-results/SUMMARY.md`. Runs automatically at the end of `task aws:bench`; this command exists for manual regen after a hand edit. |
```

If a section about per-bench artefacts exists, mention that SUMMARY.md is auto-refreshed:

> After a successful bench, the runner appends a section to `docs/benchmark-results/<connector>.md`, refreshes the marker-bounded "AWS Bench Results" section in `docs/benchmark-results/SUMMARY.md`, and writes the raw JSON under `benchmarking/aws/results/<connector>/<scenario>/<timestamp>.json`.

- [ ] **Step 2: Commit**

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws): document summary auto-refresh + task aws:summary"
```

---

## Verification checklist

- [ ] `go test ./benchmarking/aws/runner/ -run 'TestWalkResults|TestRenderSection|TestRefreshSummary' -v` — all pass
- [ ] `go vet ./benchmarking/aws/runner/` — clean
- [ ] `gofmt -l benchmarking/aws/runner/summary.go benchmarking/aws/runner/summary_test.go` — empty
- [ ] `task aws:summary` (with at least one result JSON present) — exits 0 and updates SUMMARY.md
- [ ] `git diff docs/benchmark-results/SUMMARY.md` shows ONLY the marker-bounded region changed
- [ ] Running `task aws:summary` twice in a row produces a byte-identical file the second time (idempotent)

## Done criteria

`task aws:bench` ends with both the per-connector md updated AND the SUMMARY.md AWS section refreshed in-place between markers. Hand-edited content above and below the markers stays untouched.
