// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDynamoDBConfigFieldRenames verifies the non-breaking renames of
// snapshot_segments and snapshot_batch_size to their canonical names
// (max_parallel_snapshot_tables, snapshot_max_batch_size). Both the deprecated
// and canonical names parse and resolve, defaults are preserved, and
// conflicting values are rejected. checkpoint_table is intentionally NOT
// renamed: dynamodb self-manages its checkpoint table (not a benthos cache
// resource), so it is waived in the CDC conformance test like postgres_cdc.
func TestDynamoDBConfigFieldRenames(t *testing.T) {
	spec := dynamoDBCDCInputConfig()
	parse := func(t *testing.T, yaml string) (dynamoDBCDCConfig, error) {
		t.Helper()
		p, err := spec.ParseYAML("tables: [my_table]\n"+yaml, nil)
		require.NoError(t, err)
		return dynamoCDCInputConfigFromParsed(p)
	}

	t.Run("max_parallel_snapshot_tables", func(t *testing.T) {
		t.Run("deprecated snapshot_segments", func(t *testing.T) {
			conf, err := parse(t, `snapshot_segments: 5`)
			require.NoError(t, err)
			assert.Equal(t, 5, conf.snapshot.segments)
		})
		t.Run("canonical max_parallel_snapshot_tables", func(t *testing.T) {
			conf, err := parse(t, `max_parallel_snapshot_tables: 5`)
			require.NoError(t, err)
			assert.Equal(t, 5, conf.snapshot.segments)
		})
		t.Run("default when unset", func(t *testing.T) {
			conf, err := parse(t, ``)
			require.NoError(t, err)
			assert.Equal(t, 1, conf.snapshot.segments)
		})
		t.Run("conflict", func(t *testing.T) {
			_, err := parse(t, "snapshot_segments: 5\nmax_parallel_snapshot_tables: 8")
			require.Error(t, err)
		})
	})

	t.Run("snapshot_max_batch_size", func(t *testing.T) {
		t.Run("deprecated snapshot_batch_size", func(t *testing.T) {
			conf, err := parse(t, `snapshot_batch_size: 250`)
			require.NoError(t, err)
			assert.Equal(t, 250, conf.snapshot.batchSize)
		})
		t.Run("canonical snapshot_max_batch_size", func(t *testing.T) {
			conf, err := parse(t, `snapshot_max_batch_size: 250`)
			require.NoError(t, err)
			assert.Equal(t, 250, conf.snapshot.batchSize)
		})
		t.Run("default when unset", func(t *testing.T) {
			conf, err := parse(t, ``)
			require.NoError(t, err)
			assert.Equal(t, 100, conf.snapshot.batchSize)
		})
		t.Run("conflict", func(t *testing.T) {
			_, err := parse(t, "snapshot_batch_size: 250\nsnapshot_max_batch_size: 500")
			require.Error(t, err)
		})
	})
}
