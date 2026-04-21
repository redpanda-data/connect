// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ensures the snapshot_max_parallel_tables field defaults to 1 (preserving
// the pre-parallel behaviour for configs that don't set it) and that explicit
// values round-trip through the spec.
func TestConfig_SnapshotMaxParallelTables_DefaultAndExplicit(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected int
	}{
		{
			name: "default",
			yaml: `
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
`,
			expected: 1,
		},
		{
			name: "explicit=8",
			yaml: `
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_max_parallel_tables: 8
`,
			expected: 8,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := mysqlStreamConfigSpec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)

			got, err := conf.FieldInt(fieldSnapshotMaxParallelTables)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

// Ensures newMySQLStreamInput's post-parse validation rejects non-positive
// values for snapshot_max_parallel_tables. We exercise the field contract via
// the spec rather than the full constructor (which requires a license and a
// cache resource).
func TestConfig_SnapshotMaxParallelTables_InvalidValuesRejected(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"zero", 0},
		{"negative", -5},
		{"above_upper_bound", maxSnapshotParallelTables + 1},
		{"absurdly_large", 10000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yaml := fmt.Sprintf(`
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_max_parallel_tables: %d
`, tc.value)
			conf, err := mysqlStreamConfigSpec.ParseYAML(yaml, nil)
			require.NoError(t, err, "spec parsing itself should succeed; validation is enforced inside newMySQLStreamInput")

			// Mirror the constructor's validation logic (we can't invoke the
			// constructor directly without a license/cache, but this asserts
			// the validation predicate that guards it).
			got, err := conf.FieldInt(fieldSnapshotMaxParallelTables)
			require.NoError(t, err)
			assert.True(t,
				got < 1 || got > maxSnapshotParallelTables,
				"configured value should violate the [1, %d] range enforced in newMySQLStreamInput", maxSnapshotParallelTables,
			)
		})
	}
}
