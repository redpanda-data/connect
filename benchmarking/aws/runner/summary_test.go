// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
	require.Equal(t, 4, rows[0].BestVCPU)
	require.Equal(t, 99.0, rows[0].ConnectMedianMB)
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
	require.Equal(t, 76.0, rows[0].ConnectMedianMB) // newer result's median
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
	require.Equal(t, 0.0, rows[0].ConnectMedianMB)
	require.Equal(t, 0, rows[0].BestVCPU) // 0 sentinel
}

func TestRenderSection_OneRow(t *testing.T) {
	rows := []summaryRow{
		{ConnectorScenario: "postgres / orders-cdc", BestVCPU: 4, ConnectMedianMB: 99, LastRunDate: "2026-05-21"},
	}
	var buf bytes.Buffer
	require.NoError(t, renderSection(&buf, rows, "2026-05-21"))
	out := buf.String()
	require.Contains(t, out, "## AWS Bench Results")
	require.Contains(t, out, "Last refreshed: 2026-05-21")
	require.Contains(t, out, "postgres / orders-cdc")
	require.Contains(t, out, "99")
	require.Contains(t, out, "2026-05-21")
}

func TestRenderSection_EmptyRows(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, renderSection(&buf, nil, "2026-05-21"))
	require.Contains(t, buf.String(), "*(no AWS runs yet)*")
}

func TestRenderSection_ZeroPeakShowsDash(t *testing.T) {
	rows := []summaryRow{
		{ConnectorScenario: "postgres / broken", BestVCPU: 0, ConnectMedianMB: 0, LastRunDate: "2026-05-21"},
	}
	var buf bytes.Buffer
	require.NoError(t, renderSection(&buf, rows, "2026-05-21"))
	out := buf.String()
	require.Contains(t, out, "postgres / broken")
	require.Contains(t, out, "—")
}

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
	resultsRoot := t.TempDir()
	contents := markerStart + "\n" + markerEnd
	summaryPath := writeSummaryFile(t, contents)
	require.NoError(t, RefreshSummary(summaryPath, resultsRoot, time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)))

	_, err := os.Stat(summaryPath + ".tmp")
	require.True(t, os.IsNotExist(err), "tmp file should be gone after rename")
}

func TestRefreshSummary_DualEngineRow(t *testing.T) {
	dir := t.TempDir()
	resultsRoot := filepath.Join(dir, "results")
	require.NoError(t, os.MkdirAll(filepath.Join(resultsRoot, "postgres_cdc", "orders-cdc"), 0o755))
	raw, _ := json.MarshalIndent(&Result{
		Scenario:   "postgres/orders-cdc",
		StartedAt:  time.Date(2026, 5, 19, 14, 2, 11, 0, time.UTC),
		FinishedAt: time.Date(2026, 5, 19, 15, 33, 48, 0, time.UTC),
		Points: []PointResult{
			{VCPU: 1, Engine: "connect", Summary: Summary{MedianMBPerSec: 100, PeakMBPerSec: 110}},
			{VCPU: 1, Engine: "kafka_connect", Summary: Summary{MedianMBPerSec: 72, PeakMBPerSec: 80}},
		},
	}, "", "  ")
	resultPath := filepath.Join(resultsRoot, "postgres_cdc", "orders-cdc", "2026-05-19T14-02-11Z.json")
	require.NoError(t, os.WriteFile(resultPath, raw, 0o644))

	summaryPath := filepath.Join(dir, "SUMMARY.md")
	require.NoError(t, os.WriteFile(summaryPath, []byte(SummaryMarkerStart+"\n"+SummaryMarkerEnd+"\n"), 0o644))

	require.NoError(t, RefreshSummary(summaryPath, resultsRoot, time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)))

	body, _ := os.ReadFile(summaryPath)
	s := string(body)
	require.Contains(t, s, "postgres_cdc / orders-cdc")
	// Connect median 100, KC median 72, gap +28 MB/s (+28%)
	require.Contains(t, s, "100")
	require.Contains(t, s, "72")
	require.Contains(t, s, "+28 MB/s")
	require.Contains(t, s, "+28%")
}
