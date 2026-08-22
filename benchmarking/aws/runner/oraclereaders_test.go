// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const oracleReadersScenario = "../scenarios/oracle/orders-5table-readers.yaml"

// oracleReaders returns each broker sub-input's (include list, checkpoint key)
// for a sweep point, and fails the test if the point is not shaped as a broker
// of oracledb_cdc inputs.
func oracleReaders(t *testing.T, p sweepPoint) (includes [][]string, keys []string) {
	t.Helper()
	in, ok := p.Pipeline["input"].(map[string]any)
	require.True(t, ok, "arm %s: pipeline.input must be a map", p.ArmID)
	require.Len(t, in, 1, "arm %s: input must have exactly one key, got %v — two sibling input keys is an invalid Connect config", p.ArmID, keysOf(in))

	broker, ok := in["broker"].(map[string]any)
	require.True(t, ok, "arm %s: input.broker must be a map, got %T", p.ArmID, in["broker"])
	list, ok := broker["inputs"].([]any)
	require.True(t, ok, "arm %s: broker.inputs must be a list, got %T", p.ArmID, broker["inputs"])

	for i, raw := range list {
		entry, ok := raw.(map[string]any)
		require.True(t, ok, "arm %s reader %d: entry must be a map", p.ArmID, i)
		cdc, ok := entry["oracledb_cdc"].(map[string]any)
		require.True(t, ok, "arm %s reader %d: expected an oracledb_cdc input, got %v", p.ArmID, i, keysOf(entry))

		rawInc, ok := cdc["include"].([]any)
		require.True(t, ok, "arm %s reader %d: include must be a list", p.ArmID, i)
		inc := make([]string, 0, len(rawInc))
		for _, v := range rawInc {
			s, ok := v.(string)
			require.True(t, ok, "arm %s reader %d: include entries must be strings", p.ArmID, i)
			inc = append(inc, s)
		}
		includes = append(includes, inc)

		key, ok := cdc["checkpoint_cache_key"].(string)
		require.True(t, ok, "arm %s reader %d: checkpoint_cache_key must be set explicitly", p.ArmID, i)
		keys = append(keys, key)
	}
	return includes, keys
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestOracleReaders_ArmsSweepReaderCountAtFixedCoverage is the guard on the
// experiment. The sweep only isolates "does reader COUNT change aggregate
// throughput" if every arm reads the SAME five tables, split differently. An
// arm that drops or duplicates a table would change coverage and count at once,
// and the result would mean nothing.
func TestOracleReaders_ArmsSweepReaderCountAtFixedCoverage(t *testing.T) {
	s, err := LoadScenario(oracleReadersScenario)
	require.NoError(t, err)
	require.NoError(t, s.Validate())

	plan := buildSweepPlan(s)
	require.Len(t, plan, 3, "expected 3 arms at 1 cpu point")

	wantReaders := map[string]int{
		"r1-one-reader":   1,
		"r2-two-readers":  2,
		"r5-five-readers": 5,
	}
	allTables := []string{
		"BENCH.ORDERS_T1", "BENCH.ORDERS_T2", "BENCH.ORDERS_T3",
		"BENCH.ORDERS_T4", "BENCH.ORDERS_T5",
	}

	seenArms := map[string]bool{}
	for _, p := range plan {
		want, ok := wantReaders[p.ArmID]
		require.True(t, ok, "unexpected arm id %q", p.ArmID)
		seenArms[p.ArmID] = true

		includes, keys := oracleReaders(t, p)

		// Reader count is the swept variable.
		assert.Len(t, includes, want, "arm %s must declare %d reader(s)", p.ArmID, want)

		// Coverage is the held-constant variable: the union across readers must
		// be exactly the five tables, each exactly once.
		covered := map[string]int{}
		for _, inc := range includes {
			for _, tbl := range inc {
				covered[tbl]++
			}
		}
		for _, tbl := range allTables {
			assert.Equal(t, 1, covered[tbl],
				"arm %s: table %s must be covered by exactly one reader (got %d)", p.ArmID, tbl, covered[tbl])
		}
		assert.Len(t, covered, len(allTables),
			"arm %s covers unexpected tables: %v", p.ArmID, covered)

		// Distinct checkpoint keys per reader. The readers share one memory
		// cache resource, and oracledb_cdc's default key is a constant — so
		// duplicates would have the readers overwrite each other's SCN. This is
		// silent corruption, not a startup error, which is why it is asserted.
		uniq := map[string]bool{}
		for i, k := range keys {
			assert.NotEmpty(t, k, "arm %s reader %d: empty checkpoint key", p.ArmID, i)
			assert.False(t, uniq[k], "arm %s: checkpoint_cache_key %q is reused across readers", p.ArmID, k)
			uniq[k] = true
		}
		assert.Len(t, uniq, want, "arm %s must have one distinct checkpoint key per reader", p.ArmID)
	}
	assert.Len(t, seenArms, 3, "every declared arm must appear in the plan")
}

// TestOracleReaders_ArmsHoldCoresAndLoadConstant — if arms differed in cores or
// GOMAXPROCS, a throughput change could be a CPU artifact rather than a reader
// effect. Load is scenario-level so it is constant by construction; this pins
// the per-arm half.
func TestOracleReaders_ArmsHoldCoresAndLoadConstant(t *testing.T) {
	s, err := LoadScenario(oracleReadersScenario)
	require.NoError(t, err)

	plan := buildSweepPlan(s)
	require.NotEmpty(t, plan)

	first := plan[0]
	for _, p := range plan[1:] {
		assert.Equal(t, first.VCPU, p.VCPU, "arm %s vCPU differs", p.ArmID)
		assert.Equal(t, first.GOMAXPROCS, p.GOMAXPROCS, "arm %s GOMAXPROCS differs", p.ArmID)
		assert.Equal(t, first.Streams, p.Streams, "arm %s streams differs", p.ArmID)
		assert.False(t, p.FanIn, "arm %s must not use fan_in", p.ArmID)
	}

	// Same offered load as orders-5table-split, so r1 is comparable to that
	// run's b-5tables arm.
	tables := len(s.Dataset.Tables)
	require.Equal(t, 5, tables)
	perTableMBps := float64(s.Workload.WriteRatePerSec) / float64(tables) * float64(s.Dataset.RowSizeBytes) / (1024 * 1024)
	assert.InDelta(t, 7.0, perTableMBps, 0.1,
		fmt.Sprintf("per-table load must stay ~7 MB/s for comparability (got %.2f)", perTableMBps))
}

const oracleReadersFastIOScenario = "../scenarios/oracle/orders-5table-readers-fastio.yaml"

// TestOracleReadersFastIO_MatchesBaselineExceptInfra pins the fast-I/O variant to
// being a pure infrastructure change. Its whole purpose is to test whether the
// 5-reader plateau was a storage-throughput wall, which only holds if the arms,
// load and pipeline are identical to orders-5table-readers — otherwise a
// throughput difference could come from anywhere.
func TestOracleReadersFastIO_MatchesBaselineExceptInfra(t *testing.T) {
	base, err := LoadScenario(oracleReadersScenario)
	require.NoError(t, err)
	fast, err := LoadScenario(oracleReadersFastIOScenario)
	require.NoError(t, err)
	require.NoError(t, fast.Validate())

	assert.Equal(t, base.Workload, fast.Workload, "workload must be identical for comparability")
	assert.Equal(t, base.Dataset, fast.Dataset, "dataset must be identical for comparability")
	assert.Equal(t, base.Pipeline, fast.Pipeline, "base pipeline must be identical")
	assert.Equal(t, base.Matrix, fast.Matrix, "arms must be identical — reader count is the swept variable")
	assert.Equal(t, base.Reset, fast.Reset, "reset must be identical")

	// And the infra must actually differ, or the run measures nothing new.
	assert.NotEqual(t, base.Infra.Source, fast.Infra.Source,
		"fast-io scenario must change infra.source, that is its entire purpose")
}

// TestOracleReadersFastIO_StorageSettingsAreValidForAWS catches an invalid gp3
// combination locally instead of at `terraform apply`, ~10 minutes into a paid
// run. AWS requires storage throughput in [500, 4000] MiB/s and iops >= 4x
// throughput; the instance class caps both independently.
func TestOracleReadersFastIO_StorageSettingsAreValidForAWS(t *testing.T) {
	s, err := LoadScenario(oracleReadersFastIOScenario)
	require.NoError(t, err)

	iops := asInt(s.Infra.Source["iops"])
	thr := asInt(s.Infra.Source["storage_throughput"])
	require.Positive(t, iops, "iops must be set")
	require.Positive(t, thr, "storage_throughput must be set — leaving it unset is what capped earlier runs")

	assert.GreaterOrEqual(t, thr, 500, "gp3 storage throughput floor is 500 MiB/s")
	assert.LessOrEqual(t, thr, 4000, "gp3 storage throughput ceiling is 4000 MiB/s")
	assert.GreaterOrEqual(t, iops, 4*thr,
		"AWS requires iops >= 4x storage throughput (got iops=%d, throughput=%d)", iops, thr)
}
