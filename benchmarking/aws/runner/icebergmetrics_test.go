// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"
)

func TestParseIcebergSeries_DeltaThroughput(t *testing.T) {
	dump := strings.Join([]string{
		"###timestamp=1000",
		"total_files_size_bytes 0",
		"###timestamp=1010",
		"total_files_size_bytes 104857600", // +100 MiB over 10s = 10 MiB/s
	}, "\n")
	pts, err := ParseIcebergSeries(strings.NewReader(dump))
	if err != nil {
		t.Fatalf("ParseIcebergSeries: %v", err)
	}
	if len(pts) != 1 {
		t.Fatalf("want 1 point, got %d (%#v)", len(pts), pts)
	}
	if pts[0].MBPerSec < 9.9 || pts[0].MBPerSec > 10.1 {
		t.Errorf("want ~10 MB/s, got %v", pts[0].MBPerSec)
	}
	if pts[0].T != 10 {
		t.Errorf("T = %d, want 10", pts[0].T)
	}
}

func TestParseIcebergSeries_RecordsPerSec(t *testing.T) {
	dump := strings.Join([]string{
		"###timestamp=1000",
		"total_files_size_bytes 0",
		"total_records 0",
		"###timestamp=1010",
		"total_files_size_bytes 104857600",
		"total_records 1000000", // +1M records over 10s = 100k rec/s
	}, "\n")
	pts, err := ParseIcebergSeries(strings.NewReader(dump))
	if err != nil {
		t.Fatalf("ParseIcebergSeries: %v", err)
	}
	if len(pts) != 1 {
		t.Fatalf("want 1 point, got %d (%#v)", len(pts), pts)
	}
	if pts[0].MsgPerSec < 99900 || pts[0].MsgPerSec > 100100 {
		t.Errorf("want ~100000 msg/s, got %v", pts[0].MsgPerSec)
	}
}

func TestParseIcebergSeries_SkipsCounterReset(t *testing.T) {
	dump := strings.Join([]string{
		"###timestamp=1000",
		"total_files_size_bytes 1048576",
		"###timestamp=1010",
		"total_files_size_bytes 0", // table dropped/recreated → reset; skip
	}, "\n")
	pts, err := ParseIcebergSeries(strings.NewReader(dump))
	if err != nil {
		t.Fatalf("ParseIcebergSeries: %v", err)
	}
	if len(pts) != 0 {
		t.Errorf("counter reset must yield no points, got %#v", pts)
	}
}

func TestParseIcebergSeries_IgnoresPerTableLines(t *testing.T) {
	// Finding #2 of the final whole-branch review: the sidecar now also
	// emits a per-table "table_files_size_bytes <name> <bytes>" line inside
	// each frame (live evidence for the plan's own acceptance check — a
	// zero on one of arm B's two tables means the rebalance starved one
	// stream). ParseIcebergSeries must ignore these lines and produce
	// EXACTLY the same series as a dump without them: "table_files_size_bytes"
	// is a distinct prefix from "total_files_size_bytes " (different first
	// word — "table" vs "total") and from "total_records ", so neither
	// switch case in ParseIcebergSeries can match it; it falls through to
	// the switch's default and is silently dropped.
	withoutPerTable := strings.Join([]string{
		"###timestamp=1000",
		"total_files_size_bytes 0",
		"total_records 0",
		"###timestamp=1010",
		"total_files_size_bytes 104857600",
		"total_records 1000000",
	}, "\n")
	withPerTable := strings.Join([]string{
		"###timestamp=1000",
		"table_files_size_bytes bench_sess_x_iceberg_connect_s0 0",
		"table_files_size_bytes bench_sess_x_iceberg_connect_s1 0",
		"total_files_size_bytes 0",
		"total_records 0",
		"###timestamp=1010",
		"table_files_size_bytes bench_sess_x_iceberg_connect_s0 52428800",
		"table_files_size_bytes bench_sess_x_iceberg_connect_s1 52428800",
		"total_files_size_bytes 104857600",
		"total_records 1000000",
	}, "\n")

	want, err := ParseIcebergSeries(strings.NewReader(withoutPerTable))
	if err != nil {
		t.Fatalf("ParseIcebergSeries(without): %v", err)
	}
	got, err := ParseIcebergSeries(strings.NewReader(withPerTable))
	if err != nil {
		t.Fatalf("ParseIcebergSeries(with): %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("per-table lines changed point count: got %d, want %d (got=%#v want=%#v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("point %d differs: got %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestParseIcebergSeries_Empty(t *testing.T) {
	pts, err := ParseIcebergSeries(strings.NewReader(""))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(pts) != 0 {
		t.Errorf("empty input → no points, got %#v", pts)
	}
}
