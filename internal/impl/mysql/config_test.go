// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			require.NoError(t, err)
			got, err := conf.FieldInt(fieldSnapshotMaxParallelTables)
			require.NoError(t, err)
			assert.True(t, got < 1 || got > maxSnapshotParallelTables,
				"value should violate [1, %d]", maxSnapshotParallelTables)
		})
	}
}

func TestConfig_SnapshotChunksPerTable_DefaultAndExplicit(t *testing.T) {
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
			name: "explicit=16",
			yaml: `
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_chunks_per_table: 16
`,
			expected: 16,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := mysqlStreamConfigSpec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			got, err := conf.FieldInt(fieldSnapshotChunksPerTable)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestConfig_SnapshotChunksPerTable_InvalidValuesRejected(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"zero", 0},
		{"negative", -1},
		{"above_upper_bound", maxSnapshotChunksPerTable + 1},
		{"absurdly_large", 100000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yaml := fmt.Sprintf(`
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_chunks_per_table: %d
`, tc.value)
			conf, err := mysqlStreamConfigSpec.ParseYAML(yaml, nil)
			require.NoError(t, err)
			got, err := conf.FieldInt(fieldSnapshotChunksPerTable)
			require.NoError(t, err)
			assert.True(t, got < 1 || got > maxSnapshotChunksPerTable,
				"value should violate [1, %d]", maxSnapshotChunksPerTable)
		})
	}
}
