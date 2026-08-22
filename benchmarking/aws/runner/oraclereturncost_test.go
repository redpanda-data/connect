// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const oracleReturnCostScenario = "../scenarios/oracle/orders-return-cost.yaml"

// singleReaderCeilingMBps is the measured throughput ceiling of ONE oracledb_cdc
// reader on db.r5.2xlarge: 19 MB/s median, reproduced in three independent runs
// (orders-5table-split arm b, orders-5table-readers arm r1, orders-5table-baseline
// arm r1). Every reader arm in the return-cost scenario must sit below it, or the
// arm falls behind and stops mining the same amount of stream as its comparator.
const singleReaderCeilingMBps = 19.0

// TestOracleReturnCost_ReaderArmsStayUnderTheCeiling is the validity gate for the
// whole experiment, expressed statically.
//
// The measurement is (a2 CPU - m0 CPU) = the Oracle cost of returning matched
// rows with mining held constant. That subtraction is only valid if BOTH arms
// keep pace with the redo stream — otherwise the slower arm advances through less
// redo in the fixed window and does less mining, which is exactly the flaw that
// made the previous run's R1 - M come out negative. So any reader arm here must
// be offered less than one reader can actually carry.
func TestOracleReturnCost_ReaderArmsStayUnderTheCeiling(t *testing.T) {
	s, err := LoadScenario(oracleReturnCostScenario)
	require.NoError(t, err)
	require.NoError(t, s.Validate())

	perTableMBps := float64(s.Workload.WriteRatePerSec) / float64(len(s.Dataset.Tables)) *
		float64(s.Dataset.RowSizeBytes) / (1024 * 1024)
	assert.InDelta(t, 7.0, perTableMBps, 0.1, "per-table load should stay ~7 MB/s for comparability")

	for _, p := range buildSweepPlan(s) {
		in, ok := p.Pipeline["input"].(map[string]any)
		require.True(t, ok, "arm %s must declare an input", p.ArmID)
		require.Len(t, in, 1, "arm %s must have exactly one input key, got %v", p.ArmID, keysOf(in))
		if _, isCDC := in["oracledb_cdc"]; !isCDC {
			continue // the writes-only control reads nothing
		}

		inc := oracleIncludeOf(t, p)
		offered := 0.0
		for _, tbl := range inc {
			if tbl != "BENCH.ORDERS_IDLE" { // never written, contributes no load
				offered += perTableMBps
			}
		}
		assert.Less(t, offered, singleReaderCeilingMBps,
			"arm %s is offered %.1f MB/s, at or above the measured %.1f MB/s single-reader ceiling: "+
				"it would fall behind, mine less stream than its comparator, and invalidate the subtraction",
			p.ArmID, offered, singleReaderCeilingMBps)
	}
}

// TestOracleReturnCost_ArmsIsolateReturnCost pins the three arms to the roles the
// decomposition needs: no reader, a reader returning nothing, and a reader
// returning a known non-zero volume.
func TestOracleReturnCost_ArmsIsolateReturnCost(t *testing.T) {
	s, err := LoadScenario(oracleReturnCostScenario)
	require.NoError(t, err)

	_, baseHasInput := s.Pipeline["input"]
	assert.False(t, baseHasInput,
		"base must not declare an input: a base input key cannot be removed by an arm override and "+
			"would leak into the writes-only arm as a second input type")

	written := map[string]bool{}
	for _, tbl := range s.Dataset.Tables {
		written["BENCH."+upper(tbl)] = true
	}

	byArm := map[string]sweepPoint{}
	for _, p := range buildSweepPlan(s) {
		byArm[p.ArmID] = p
	}
	require.Len(t, byArm, 3)

	// w0 — reads nothing from Oracle.
	w0, ok := byArm["w0-writes-only"]
	require.True(t, ok)
	w0in, _ := w0.Pipeline["input"].(map[string]any)
	_, hasCDC := w0in["oracledb_cdc"]
	assert.False(t, hasCDC, "w0 must not read Oracle at all")

	// m0 — exactly one table, and it must be one the workload never writes.
	m0, ok := byArm["m0-zero-tables"]
	require.True(t, ok)
	m0inc := oracleIncludeOf(t, m0)
	require.Len(t, m0inc, 1)
	assert.False(t, written[m0inc[0]],
		"m0 reads %s, which the workload writes — m0 must target an idle table so it returns no DML", m0inc[0])

	// a2 — two written tables, so it returns a known non-zero volume.
	a2, ok := byArm["a2-two-tables"]
	require.True(t, ok)
	a2inc := oracleIncludeOf(t, a2)
	assert.Len(t, a2inc, 2, "a2 must read two tables")
	for _, tbl := range a2inc {
		assert.True(t, written[tbl], "a2 includes %s, which the workload does not write", tbl)
	}

	// Distinct checkpoint keys: the reader arms share one memory cache resource
	// and oracledb_cdc's default key is a constant, so a collision would have
	// them overwrite each other's SCN across sweep points.
	assert.NotEqual(t,
		m0.Pipeline["input"].(map[string]any)["oracledb_cdc"].(map[string]any)["checkpoint_cache_key"],
		a2.Pipeline["input"].(map[string]any)["oracledb_cdc"].(map[string]any)["checkpoint_cache_key"],
		"reader arms must use distinct checkpoint_cache_key values")
}

// TestOracleReturnCost_WindowIsLongEnoughForSensitivity guards the reason the
// window was doubled. The effect under measurement was previously estimated at
// only 1-2 CPU points against per-minute noise of roughly +/-8, so the run needs
// enough one-minute CloudWatch samples per arm to resolve it. Shortening the
// window back to 15m would quietly halve the sample count and make the result
// unreadable.
func TestOracleReturnCost_WindowIsLongEnoughForSensitivity(t *testing.T) {
	s, err := LoadScenario(oracleReturnCostScenario)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, s.Workload.Duration, 30*time.Minute,
		"window must be >= 30m: the CPU difference being measured is small relative to per-minute noise")
}
