// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sampleResult() *Result {
	return &Result{
		Scenario:     "postgres/orders-cdc",
		ScenarioHash: "sha256:abc123",
		GitSHA:       "e491c80fc",
		StartedAt:    time.Date(2026, 5, 19, 14, 2, 11, 0, time.UTC),
		FinishedAt:   time.Date(2026, 5, 19, 15, 33, 48, 0, time.UTC),
		Infra: ResultInfra{
			RunnerInstanceType:  "c7i.4xlarge",
			SourceInstanceClass: "db.r6g.2xlarge",
			SourceStorageGB:     400,
			Region:              "us-east-2",
		},
		Dataset: ResultDataset{Rows: 75_000_000, RowSizeBytes: 1200, TotalBytes: 90_000_000_000},
		Points: []PointResult{
			{
				VCPU:    1,
				Engine:  "connect",
				Samples: []Sample{{T: 0, MBPerSec: 153, MsgPerSec: 127344}},
				Summary: Summary{
					MedianMBPerSec: 153, P5MBPerSec: 144, P95MBPerSec: 161, PeakMBPerSec: 167,
					MeanMBPerSec:    0.123,
					MedianMsgPerSec: 127344, P5MsgPerSec: 119800, P95MsgPerSec: 134000, PeakMsgPerSec: 138200,
					MeanMsgPerSec: 125000,
				},
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

func TestAppendMarkdown_PointOrderingIsRobust(t *testing.T) {
	// Build the same result two ways: two points in one order versus the
	// reverse order. The rendering is one row per point in result order, so
	// swapping the points must swap the rendered rows too.
	r1 := sampleResult()
	r1.Points = append(r1.Points, PointResult{
		VCPU: 2, Engine: "connect", Summary: Summary{MedianMBPerSec: 200},
	})
	r2 := sampleResult()
	r2.Points = []PointResult{r1.Points[1], r1.Points[0]}

	dir := t.TempDir()
	target1 := filepath.Join(dir, "r1.md")
	target2 := filepath.Join(dir, "r2.md")
	require.NoError(t, os.WriteFile(target1, []byte{}, 0o644))
	require.NoError(t, os.WriteFile(target2, []byte{}, 0o644))
	require.NoError(t, AppendMarkdown(target1, r1, "desc"))
	require.NoError(t, AppendMarkdown(target2, r2, "desc"))

	b1, err := os.ReadFile(target1)
	require.NoError(t, err)
	b2, err := os.ReadFile(target2)
	require.NoError(t, err)
	require.NotEqual(t, string(b1), string(b2),
		"rows render in result order, so swapping the points must change the output")
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
	// New header columns:
	require.Contains(t, s, "engine")
	require.Contains(t, s, "broker MB/s")
	// Mean throughput column (3-decimal so sub-1 sink rates show):
	require.Contains(t, s, "mean MB/s")
	require.Contains(t, s, "0.123", "expected 3-decimal mean MB/s value in row; full markdown:\n"+s)
	// Mean records/sec column (compression-independent sink throughput):
	require.Contains(t, s, "mean msg/s")
	require.Contains(t, s, "125,000", "expected formatted mean msg/s value in row; full markdown:\n"+s)
	// The engine label appears in the row:
	require.Contains(t, s, "connect")
	// The vCPU row still renders the median:
	require.Contains(t, s, "153")
	// msg/sec (p50) — formatted with thousands separator:
	require.Contains(t, s, "127,344")
}

func TestWriteResultJSON_RoundTripsStreams(t *testing.T) {
	// Finding #5: Streams must survive the JSON round-trip alongside Arm and
	// GOMAXPROCS, so a result file is re-analysable without inferring the
	// stream count from the arm-id naming convention.
	dir := t.TempDir()
	r := sampleResult()
	r.Points[0].Arm = "b-2pipe-gmp4"
	r.Points[0].GOMAXPROCS = 4
	r.Points[0].Streams = 2
	path, err := WriteResultJSON(dir, r)
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"streams": 2`)

	var got Result
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, 2, got.Points[0].Streams)
}

func TestAppendMarkdown_RendersArmRows(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "iceberg.md")
	r := &Result{
		Scenario:  "iceberg/orders-sink-streams-ab",
		GitSHA:    "abcdef1234567890",
		StartedAt: time.Now().UTC(),
		Points: []PointResult{
			{
				VCPU: 2, GOMAXPROCS: 2, Arm: "a0-1pipe-gmp2", Engine: "connect",
				Summary: Summary{MedianMBPerSec: 69, MeanMBPerSec: 69.1},
			},
			{
				VCPU: 2, GOMAXPROCS: 4, Arm: "a1-1pipe-gmp4", Engine: "connect",
				Summary: Summary{MedianMBPerSec: 80, MeanMBPerSec: 80.2},
			},
			{
				VCPU: 2, GOMAXPROCS: 4, Arm: "b-2pipe-gmp4", Engine: "connect",
				Summary: Summary{MedianMBPerSec: 95, MeanMBPerSec: 95.3},
			},
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
	// Count "| connect" (row-leading pipe + engine column) rather than bare
	// "connect": the header also links to
	// https://github.com/redpanda-data/connect/commit/... which contains the
	// substring "connect" and would otherwise inflate the count by one.
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
			{VCPU: 1, GOMAXPROCS: 1, Engine: "connect", Summary: Summary{MedianMBPerSec: 10, MedianMsgPerSec: 10000}},
		},
	}
	require.NoError(t, AppendMarkdown(target, r, "cdc"))
	raw, err := os.ReadFile(target)
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, "| connect", "arm-less row still renders under the blank arm column")
	require.Contains(t, out, "10,000", "median msg/s formats with a thousands separator")
}
