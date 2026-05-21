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
