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

const oracleBaselineScenario = "../scenarios/oracle/orders-5table-baseline.yaml"

// TestOracleBaseline_EachArmHasExactlyOneInput guards the trap this scenario was
// built around. mergePipeline recurses into maps, so it can only add or replace
// keys — never remove them. A base `pipeline.input.oracledb_cdc` would survive
// into the writes-only arm alongside its `generate`, yielding a config with two
// sibling input types. Connect rejects that, but only when it starts — roughly
// ten minutes into a run, after the RDS instance has been provisioned and paid
// for. Hence the base carries no input at all and every arm declares its own.
func TestOracleBaseline_EachArmHasExactlyOneInput(t *testing.T) {
	s, err := LoadScenario(oracleBaselineScenario)
	require.NoError(t, err)
	require.NoError(t, s.Validate())

	// The base must not declare an input, or the guarantee below cannot hold.
	_, baseHasInput := s.Pipeline["input"]
	assert.False(t, baseHasInput,
		"scenario pipeline must NOT declare an input: a base input key cannot be removed by an arm override, "+
			"and would leak into the writes-only arm as a second sibling input type")

	plan := buildSweepPlan(s)
	require.Len(t, plan, 3)

	wantInput := map[string]string{
		"m1-empty-reader": "oracledb_cdc",
		"r1-one-reader":   "oracledb_cdc",
		"w0-writes-only":  "generate",
	}

	for _, p := range plan {
		in, ok := p.Pipeline["input"].(map[string]any)
		require.True(t, ok, "arm %s: rendered point must declare an input", p.ArmID)
		require.Len(t, in, 1,
			"arm %s: must have EXACTLY ONE input key, got %v — Connect rejects sibling input types", p.ArmID, keysOf(in))

		want, ok := wantInput[p.ArmID]
		require.True(t, ok, "unexpected arm %q", p.ArmID)
		_, present := in[want]
		assert.True(t, present, "arm %s: expected input %q, got %v", p.ArmID, want, keysOf(in))
	}
}

// TestOracleBaseline_DecompositionArmsAreDistinguishable checks that the three
// arms actually isolate what the decomposition needs: w0 must not touch Oracle
// at all, m1 must read only the never-written table, and r1 must read all five
// written ones. If m1 ever pointed at a written table it would stop measuring
// "mining with zero rows returned" and the subtraction would be meaningless.
func TestOracleBaseline_DecompositionArmsAreDistinguishable(t *testing.T) {
	s, err := LoadScenario(oracleBaselineScenario)
	require.NoError(t, err)

	written := map[string]bool{}
	for _, tbl := range s.Dataset.Tables {
		written["BENCH."+upper(tbl)] = true
	}
	require.Len(t, written, 5)

	byArm := map[string]sweepPoint{}
	for _, p := range buildSweepPlan(s) {
		byArm[p.ArmID] = p
	}

	// w0 — no Oracle reader whatsoever.
	w0 := byArm["w0-writes-only"]
	in, _ := w0.Pipeline["input"].(map[string]any)
	_, hasCDC := in["oracledb_cdc"]
	assert.False(t, hasCDC, "w0 must not declare an oracledb_cdc input — its window measures write CPU alone")

	// m1 — reads exactly one table, and that table must NOT be written to.
	m1inc := oracleIncludeOf(t, byArm["m1-empty-reader"])
	require.Len(t, m1inc, 1, "m1 must read exactly one table")
	assert.False(t, written[m1inc[0]],
		"m1 reads %s, which IS written by the workload — m1 must target an idle table so it returns zero rows", m1inc[0])

	// r1 — reads all five written tables.
	r1inc := oracleIncludeOf(t, byArm["r1-one-reader"])
	assert.Len(t, r1inc, 5, "r1 must read all five written tables")
	for _, tbl := range r1inc {
		assert.True(t, written[tbl], "r1 includes %s, which the workload does not write", tbl)
	}
}

func oracleIncludeOf(t *testing.T, p sweepPoint) []string {
	t.Helper()
	in, ok := p.Pipeline["input"].(map[string]any)
	require.True(t, ok, "arm %s: input must be a map", p.ArmID)
	cdc, ok := in["oracledb_cdc"].(map[string]any)
	require.True(t, ok, "arm %s: expected an oracledb_cdc input", p.ArmID)
	raw, ok := cdc["include"].([]any)
	require.True(t, ok, "arm %s: include must be a list", p.ArmID)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, ok := v.(string)
		require.True(t, ok, "arm %s: include entries must be strings", p.ArmID)
		out = append(out, s)
	}
	return out
}

// upper uppercases an ASCII identifier, mirroring how Oracle folds unquoted
// table names (the scenario writes them lowercase in dataset.tables and
// uppercase in the include lists).
func upper(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'a' && b[i] <= 'z' {
			b[i] -= 32
		}
	}
	return string(b)
}
