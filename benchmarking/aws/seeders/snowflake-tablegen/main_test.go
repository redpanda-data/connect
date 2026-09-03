// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "testing"

// The frame body must match what ParseIcebergSeries (runner/icebergmetrics.go)
// consumes: total_files_size_bytes and total_records lines, with the
// per-table lines a distinct prefix the parser silently drops.
func TestFormatFrame(t *testing.T) {
	stats := map[string]tableStat{
		"BENCH_SESS_SNOWFLAKE_CONNECT_S0": {rows: 100, bytes: 4096},
		"BENCH_SESS_SNOWFLAKE_CONNECT_S1": {rows: 50, bytes: 1024},
	}
	got := formatFrame([]string{"bench_sess_snowflake_connect_s0", "bench_sess_snowflake_connect_s1"}, stats)
	want := `table_files_size_bytes bench_sess_snowflake_connect_s0 4096
table_files_size_bytes bench_sess_snowflake_connect_s1 1024
total_files_size_bytes 5120
total_records 150
`
	if got != want {
		t.Errorf("formatFrame:\n got: %q\nwant: %q", got, want)
	}
}

func TestFormatFrame_MissingTableCountsZero(t *testing.T) {
	got := formatFrame([]string{"bench_missing"}, map[string]tableStat{})
	want := `table_files_size_bytes bench_missing 0
total_files_size_bytes 0
total_records 0
`
	if got != want {
		t.Errorf("formatFrame(missing):\n got: %q\nwant: %q", got, want)
	}
}

func TestToInt64(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want int64
	}{
		{int64(42), 42},
		{float64(42.9), 42},
		{"42", 42},
		{nil, 0},
		{"", 0},
	} {
		if got := toInt64(tc.in); got != tc.want {
			t.Errorf("toInt64(%#v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestSnowflakeIdent(t *testing.T) {
	if !snowflakeIdent.MatchString("bench_sess_1_snowflake_connect") {
		t.Error("sanitized bench table names must validate")
	}
	for _, bad := range []string{"bench-dash", "1leading", `x"; DROP TABLE y`, ""} {
		if snowflakeIdent.MatchString(bad) {
			t.Errorf("%q must not validate", bad)
		}
	}
}
