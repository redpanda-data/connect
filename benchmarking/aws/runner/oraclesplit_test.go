// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oracleSplitScenario is the real scenario file, not a fixture: the whole value
// of this test is that the shipped YAML expands into the two arms the
// experiment needs.
const oracleSplitScenario = "../scenarios/oracle/orders-5table-split.yaml"

func oracleSplitInclude(t *testing.T, p sweepPoint) []any {
	t.Helper()
	in, ok := p.Pipeline["input"].(map[string]any)
	require.True(t, ok, "arm %s: pipeline.input must be a map", p.ArmID)
	cdc, ok := in["oracledb_cdc"].(map[string]any)
	require.True(t, ok, "arm %s: input.oracledb_cdc must be a map", p.ArmID)
	inc, ok := cdc["include"].([]any)
	require.True(t, ok, "arm %s: include must be a list, got %T", p.ArmID, cdc["include"])
	return inc
}

// TestOracleSplitArms_DifferOnlyByIncludeList is the guard on the experiment's
// validity. The arms answer "is the oracledb_cdc ceiling stream-traversal or
// per-session parse of matched rows?" only if they are identical in every
// respect except how many of the 5 loaded tables each mines. If a later edit
// gives the arms different core counts, batching, or checkpoint settings, the
// comparison silently stops meaning that and the run wastes an RDS Oracle
// provision.
func TestOracleSplitArms_DifferOnlyByIncludeList(t *testing.T) {
	s, err := LoadScenario(oracleSplitScenario)
	require.NoError(t, err)
	require.NoError(t, s.Validate())

	plan := buildSweepPlan(s)
	require.Len(t, plan, 2, "expected exactly 2 arms at 1 cpu point")

	byArm := map[string]sweepPoint{}
	for _, p := range plan {
		byArm[p.ArmID] = p
	}
	a, ok := byArm["a-1table"]
	require.True(t, ok, "arm a-1table missing from plan")
	b, ok := byArm["b-5tables"]
	require.True(t, ok, "arm b-5tables missing from plan")

	// The one intended difference.
	assert.Len(t, oracleSplitInclude(t, a), 1, "arm a must mine exactly one table")
	assert.Len(t, oracleSplitInclude(t, b), 5, "arm b must mine all five tables")

	// Everything that must NOT differ. Unequal cores or GOMAXPROCS would make a
	// shortfall in arm b indistinguishable from CPU starvation.
	assert.Equal(t, a.VCPU, b.VCPU, "arms must share a vCPU pin")
	assert.Equal(t, a.GOMAXPROCS, b.GOMAXPROCS, "arms must share GOMAXPROCS")
	assert.Equal(t, a.Streams, b.Streams, "arms must both be single-pipeline")
	assert.False(t, a.FanIn || b.FanIn, "fan-in is meaningless for a CDC source")

	// Same input tuning on both sides, so only the table filter varies.
	aCDC := a.Pipeline["input"].(map[string]any)["oracledb_cdc"].(map[string]any)
	bCDC := b.Pipeline["input"].(map[string]any)["oracledb_cdc"].(map[string]any)
	for _, k := range []string{"stream_snapshot", "checkpoint_cache", "checkpoint_cache_key", "batching", "connection_string"} {
		assert.Equal(t, bCDC[k], aCDC[k], "arms must share input field %q", k)
	}
}

// TestOracleSplitArms_ArmOverrideDoesNotLeak pins the mergePipeline behaviour
// the scenario depends on: `include` is a sequence, so an arm's one-element
// list must REPLACE the base's five rather than merge into it. If sequences
// ever started merging, arm a would quietly mine all five tables and both arms
// would measure the same thing.
func TestOracleSplitArms_ArmOverrideDoesNotLeak(t *testing.T) {
	s, err := LoadScenario(oracleSplitScenario)
	require.NoError(t, err)

	plan := buildSweepPlan(s)
	require.Len(t, plan, 2)

	// The scenario's own pipeline must be untouched by the merge — the base
	// carries all five tables and is reused for the second arm.
	baseInc := s.Pipeline["input"].(map[string]any)["oracledb_cdc"].(map[string]any)["include"].([]any)
	assert.Len(t, baseInc, 5, "arm merge must not mutate the scenario's base include list")

	for _, p := range plan {
		if p.ArmID != "a-1table" {
			continue
		}
		inc := oracleSplitInclude(t, p)
		require.Len(t, inc, 1)
		assert.Equal(t, "BENCH.ORDERS_T1", inc[0])
	}
}

// TestOracleSplitScenario_LoadMatchesPerTableTarget guards the arithmetic that
// makes the result readable. The experiment asks whether a session can keep up
// with ~7 MB/s per table across 5 tables; if write_rate_per_sec and
// row_size_bytes drift out of that relationship, the arms still run but the
// numbers no longer answer the question.
//
// cdc-rows-oracle's -rate is rows/sec TOTAL across tables (it divides evenly),
// unlike the postgres/mysql seeders where the scenario field is per-table.
func TestOracleSplitScenario_LoadMatchesPerTableTarget(t *testing.T) {
	s, err := LoadScenario(oracleSplitScenario)
	require.NoError(t, err)

	tables := len(s.Dataset.Tables)
	require.Equal(t, 5, tables)

	perTableRows := float64(s.Workload.WriteRatePerSec) / float64(tables)
	perTableMBps := perTableRows * float64(s.Dataset.RowSizeBytes) / (1024 * 1024)

	assert.InDelta(t, 7.0, perTableMBps, 0.1,
		"per-table load must be ~7 MB/s (got %.2f); adjust write_rate_per_sec or row_size_bytes together", perTableMBps)

	totalMBps := perTableMBps * float64(tables)
	assert.InDelta(t, 35.0, totalMBps, 0.5,
		"total load must be ~35 MB/s (got %.2f) — above the ~19 MB/s the old runs reported, or the ceiling question stays untested", totalMBps)
}
