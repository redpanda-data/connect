// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslateInfraSource(t *testing.T) {
	const region = "us-east-2"
	tests := []struct {
		name string
		src  map[string]any
		key  string
		want string
	}{
		{
			name: "string passes through",
			src:  map[string]any{"table_name": "orders"},
			key:  "table_name",
			want: "orders",
		},
		{
			name: "int formats as decimal",
			src:  map[string]any{"write_capacity": 40000},
			key:  "write_capacity",
			want: "40000",
		},
		{
			name: "slice JSON-encodes to an HCL list literal",
			src:  map[string]any{"table_names": []any{"a", "b", "c"}},
			key:  "table_names",
			want: `["a","b","c"]`,
		},
		{
			name: "empty slice encodes to empty list",
			src:  map[string]any{"table_names": []any{}},
			key:  "table_names",
			want: "[]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translateInfraSource(tt.src, region)
			if got[tt.key] != tt.want {
				t.Errorf("translateInfraSource[%q] = %q, want %q", tt.key, got[tt.key], tt.want)
			}
			if got["region"] != region {
				t.Errorf("region = %q, want %q", got["region"], region)
			}
		})
	}
}

func binaryArmScenario() *Scenario {
	return &Scenario{
		Name: "soak-x",
		Matrix: MatrixSpec{
			CPUPoints: []int{2},
			Arms:      []Arm{{ID: "base", Binary: "base"}, {ID: "pr", Binary: "pr"}},
		},
	}
}

// TestValidateBinaryFlags_AcceptsCompleteMapping is the happy path: every
// arm-referenced binary has a mapping and every mapping is referenced.
func TestValidateBinaryFlags_AcceptsCompleteMapping(t *testing.T) {
	s := binaryArmScenario()
	err := validateBinaryFlags(s, map[string]string{"base": "/tmp/base", "pr": "/tmp/pr"})
	require.NoError(t, err)
}

// TestValidateBinaryFlags_AcceptsNoArmsNoBinaries covers every scenario
// before CON-179 R6 increment 5: no matrix.arms[].binary means no --binary
// flags are required.
func TestValidateBinaryFlags_AcceptsNoArmsNoBinaries(t *testing.T) {
	require.NoError(t, validateBinaryFlags(&Scenario{Matrix: MatrixSpec{Arms: []Arm{{ID: "a0"}}}}, nil))
}

// TestValidateBinaryFlags_RejectsMissingMapping is the "validate before any
// AWS spend" case: an arm references a binary with no --binary flag at all.
func TestValidateBinaryFlags_RejectsMissingMapping(t *testing.T) {
	s := binaryArmScenario()
	err := validateBinaryFlags(s, map[string]string{"base": "/tmp/base"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "pr")
	require.Contains(t, err.Error(), "no matching --binary mapping")
}

// TestValidateBinaryFlags_RejectsUnreferencedMapping is the typo guard: a
// --binary flag for a name no arm actually references (e.g. "bas" instead
// of "base").
func TestValidateBinaryFlags_RejectsUnreferencedMapping(t *testing.T) {
	s := binaryArmScenario()
	err := validateBinaryFlags(s, map[string]string{"base": "/tmp/base", "pr": "/tmp/pr", "bas": "/tmp/typo"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bas")
	require.Contains(t, err.Error(), "likely a typo")
}

// TestValidateBinaryFlags_RejectsUnreferencedMappingWithNoArms covers a
// --binary flag passed against a scenario with no binary arms at all —
// still a typo guard, just against a scenario that couldn't use the
// mapping under any circumstance.
func TestValidateBinaryFlags_RejectsUnreferencedMappingWithNoArms(t *testing.T) {
	err := validateBinaryFlags(&Scenario{Matrix: MatrixSpec{Arms: []Arm{{ID: "a0"}}}}, map[string]string{"base": "/tmp/base"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "base")
}

func TestBinaryFlag_AccumulatesRepeatedFlags(t *testing.T) {
	f := &binaryFlag{}
	require.NoError(t, f.Set("base=/tmp/base"))
	require.NoError(t, f.Set("pr=/tmp/pr"))
	require.Equal(t, map[string]string{"base": "/tmp/base", "pr": "/tmp/pr"}, f.m)
}

func TestBinaryFlag_RejectsMalformedValue(t *testing.T) {
	f := &binaryFlag{}
	require.Error(t, f.Set("no-equals-sign"))
	require.Error(t, f.Set("=/tmp/x"), "empty name")
	require.Error(t, f.Set("name="), "empty path")
}

func TestBinaryFlag_RejectsDuplicateName(t *testing.T) {
	f := &binaryFlag{}
	require.NoError(t, f.Set("base=/tmp/base"))
	err := f.Set("base=/tmp/other")
	require.Error(t, err)
	require.Contains(t, err.Error(), "more than once")
}

// TestBuildBinaryStagePlan_LegacyKeepsHistoricalPath is the parity guard for
// stageArtefacts' script rendering: no --binary mappings means the
// single-binary stage/download/chmod shape stays byte-for-byte identical to
// before matrix.arms[].binary existed.
func TestBuildBinaryStagePlan_LegacyKeepsHistoricalPath(t *testing.T) {
	items, download, chmod := buildBinaryStagePlan(nil, "/local/tmp/rpcn", "b")
	require.Equal(t, []upload{{"stage/redpanda-connect", "/local/tmp/rpcn"}}, items)
	require.Equal(t, "aws s3 cp s3://b/stage/redpanda-connect /opt/bench/redpanda-connect", download)
	require.Equal(t, "chmod +x /opt/bench/redpanda-connect", chmod)
}

// TestBuildBinaryStagePlan_NamedBinariesAgreeWithRunnerBinaryPath pins the
// staged S3 key, the host download destination, and MatrixRunner.
// binaryPathFor's launch path (matrix.go) to the SAME value for every named
// binary — a drift there means the engine launches a path that was never
// downloaded.
func TestBuildBinaryStagePlan_NamedBinariesAgreeWithRunnerBinaryPath(t *testing.T) {
	binaries := map[string]string{"pr": "/local/tmp/pr-bin", "base": "/local/tmp/base-bin"}
	items, download, chmod := buildBinaryStagePlan(binaries, "", "b")

	// Sorted by name regardless of map iteration order.
	require.Equal(t, []upload{
		{"stage/redpanda-connect-base", "/local/tmp/base-bin"},
		{"stage/redpanda-connect-pr", "/local/tmp/pr-bin"},
	}, items)

	mr := &MatrixRunner{}
	for _, name := range []string{"base", "pr"} {
		launchPath := mr.binaryPathFor(sweepPoint{Binary: name})
		require.Equal(t, runnerBinaryPath(name), launchPath)
		require.Contains(t, download, fmt.Sprintf("aws s3 cp s3://b/%s %s", stageBinaryKey(name), launchPath))
		require.Contains(t, chmod, fmt.Sprintf("chmod +x %s", launchPath))
	}
}

func TestSortedBinaryNames(t *testing.T) {
	require.Equal(t, []string{"base", "pr"}, sortedBinaryNames(map[string]string{"pr": "/x", "base": "/y"}))
	require.Empty(t, sortedBinaryNames(nil))
}
