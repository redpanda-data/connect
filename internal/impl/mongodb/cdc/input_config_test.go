// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/cdcfield"
)

const mongoBaseConfig = `
url: mongodb://localhost:27017
database: test
collections: [ my_collection ]
checkpoint_cache: my_cache
`

func mongoParse(t *testing.T, yaml string) *service.ParsedConfig {
	t.Helper()
	conf, err := spec().ParseYAML(mongoBaseConfig+yaml, nil)
	require.NoError(t, err)
	return conf
}

// TestMongoMaxParallelSnapshotFieldNames verifies the non-breaking rename of
// snapshot_parallelism to the canonical max_parallel_snapshot_tables: both
// names parse and resolve, and conflicting values are rejected.
func TestMongoMaxParallelSnapshotFieldNames(t *testing.T) {
	resolve := func(t *testing.T, yaml string) (int, error) {
		return cdcfield.ResolveInt(mongoParse(t, yaml), fieldMaxParallelSnapshot, fieldSnapshotParallelism, 1)
	}

	t.Run("deprecated name", func(t *testing.T) {
		v, err := resolve(t, `snapshot_parallelism: 4`)
		require.NoError(t, err)
		assert.Equal(t, 4, v)
	})
	t.Run("canonical name", func(t *testing.T) {
		v, err := resolve(t, `max_parallel_snapshot_tables: 4`)
		require.NoError(t, err)
		assert.Equal(t, 4, v)
	})
	t.Run("default when unset", func(t *testing.T) {
		v, err := resolve(t, ``)
		require.NoError(t, err)
		assert.Equal(t, 1, v)
	})
	t.Run("both set to same value", func(t *testing.T) {
		v, err := resolve(t, "snapshot_parallelism: 4\nmax_parallel_snapshot_tables: 4")
		require.NoError(t, err)
		assert.Equal(t, 4, v)
	})
	t.Run("both set to conflicting values", func(t *testing.T) {
		_, err := resolve(t, "snapshot_parallelism: 4\nmax_parallel_snapshot_tables: 8")
		require.Error(t, err)
	})
}

// TestMongoSnapshotMaxBatchSizeField verifies the new canonical
// snapshot_max_batch_size field parses with the expected default and override.
func TestMongoSnapshotMaxBatchSizeField(t *testing.T) {
	t.Run("default when unset", func(t *testing.T) {
		v, err := mongoParse(t, ``).FieldInt(fieldSnapshotMaxBatchSize)
		require.NoError(t, err)
		assert.Equal(t, 1000, v)
	})
	t.Run("explicit value", func(t *testing.T) {
		v, err := mongoParse(t, `snapshot_max_batch_size: 250`).FieldInt(fieldSnapshotMaxBatchSize)
		require.NoError(t, err)
		assert.Equal(t, 250, v)
	})
}
