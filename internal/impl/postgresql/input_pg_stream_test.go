// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/cdcfield"
)

// TestPostgresSnapshotBatchSizeFieldNames verifies the non-breaking rename of
// snapshot_batch_size to the canonical snapshot_max_batch_size: both names parse
// and resolve, and setting both to conflicting values is rejected.
func TestPostgresSnapshotBatchSizeFieldNames(t *testing.T) {
	spec := newPostgresCDCConfig()
	const base = `
dsn: postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable
schema: public
tables: [my_table]
slot_name: my_slot
`
	resolve := func(t *testing.T, yaml string) (int, error) {
		t.Helper()
		conf, err := spec.ParseYAML(base+yaml, nil)
		require.NoError(t, err)
		return cdcfield.ResolveInt(conf, fieldSnapshotMaxBatchSize, fieldSnapshotBatchSize, 1000)
	}

	t.Run("deprecated name", func(t *testing.T) {
		v, err := resolve(t, `snapshot_batch_size: 250`)
		require.NoError(t, err)
		assert.Equal(t, 250, v)
	})
	t.Run("canonical name", func(t *testing.T) {
		v, err := resolve(t, `snapshot_max_batch_size: 250`)
		require.NoError(t, err)
		assert.Equal(t, 250, v)
	})
	t.Run("default when unset", func(t *testing.T) {
		v, err := resolve(t, ``)
		require.NoError(t, err)
		assert.Equal(t, 1000, v)
	})
	t.Run("both set to same value", func(t *testing.T) {
		v, err := resolve(t, "snapshot_batch_size: 250\nsnapshot_max_batch_size: 250")
		require.NoError(t, err)
		assert.Equal(t, 250, v)
	})
	t.Run("both set to conflicting values", func(t *testing.T) {
		_, err := resolve(t, "snapshot_batch_size: 250\nsnapshot_max_batch_size: 500")
		require.Error(t, err)
	})
}
