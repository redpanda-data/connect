// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package migratortest provides V1↔V2 equivalence helpers for the connect
// Bloblang plugin set. AssertEquivalent runs a V1 mapping under V1, translates
// it via the connect migrator rule pack, then runs the resulting V2 mapping
// under V2, and asserts both produce the same output. Use it from per-plugin
// test files to catch silent strictness or coercion divergences between the V1
// and V2 ports.
package migratortest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
	bloblmig "github.com/redpanda-data/benthos/v4/public/bloblangv2/migrator"

	"github.com/redpanda-data/connect/v4/internal/bloblang/migratorrules"
)

// AssertEquivalent runs v1Mapping under V1 and the migrator-translated V2
// equivalent under V2. Both queries receive the supplied input. The helper
// asserts both invocations succeed and that their outputs compare equal under
// reflect.DeepEqual.
//
// Most connect plugins are pure transforms; if a mapping mutates its input
// the helper's two queries may interfere — pass a freshly-constructed input
// in that case (or use AssertEquivalentFn).
func AssertEquivalent(t *testing.T, v1Mapping string, input, expected any) {
	t.Helper()
	AssertEquivalentFn(t, v1Mapping, func() any { return input }, expected)
}

// AssertEquivalentFn is AssertEquivalent for inputs that the helper must
// reconstruct between calls (e.g. maps a plugin might mutate). makeInput is
// invoked twice: once for the V1 query, once for the V2 query.
func AssertEquivalentFn(t *testing.T, v1Mapping string, makeInput func() any, expected any) {
	t.Helper()

	// V1 evaluation.
	v1Exec, err := bloblang.Parse(v1Mapping)
	require.NoErrorf(t, err, "V1 parse failed for mapping:\n%s", v1Mapping)
	v1Got, err := v1Exec.Query(makeInput())
	require.NoErrorf(t, err, "V1 query failed for mapping:\n%s", v1Mapping)

	// Translate V1 → V2 via the connect migrator rule pack.
	mig := bloblmig.New()
	migratorrules.Register(mig)
	rep, err := mig.Migrate(v1Mapping, bloblmig.Options{})
	require.NoErrorf(t, err, "migrator failed for mapping:\n%s", v1Mapping)

	// V2 evaluation.
	v2Exec, err := bloblangv2.Parse(rep.V2Mapping)
	require.NoErrorf(t, err, "V2 parse failed for migrated mapping:\n%s", rep.V2Mapping)
	v2Got, err := v2Exec.Query(makeInput())
	require.NoErrorf(t, err, "V2 query failed for migrated mapping:\n%s", rep.V2Mapping)

	assert.Equalf(t, expected, v1Got, "V1 result mismatch for mapping:\n%s", v1Mapping)
	assert.Equalf(t, expected, v2Got, "V2 result mismatch for mapping:\n%s\n(migrated to:\n%s)", v1Mapping, rep.V2Mapping)
	assert.Equalf(t, v1Got, v2Got, "V1↔V2 divergence for mapping:\n%s\nV1=%#v\nV2=%#v", v1Mapping, v1Got, v2Got)
}
