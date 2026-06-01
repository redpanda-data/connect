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

func TestParseIcebergSeries_Empty(t *testing.T) {
	pts, err := ParseIcebergSeries(strings.NewReader(""))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(pts) != 0 {
		t.Errorf("empty input → no points, got %#v", pts)
	}
}
