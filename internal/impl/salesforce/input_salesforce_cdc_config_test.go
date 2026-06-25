// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/cdcfield"
)

// TestSalesforceMaxParallelSnapshotFieldNames verifies the non-breaking rename
// of max_parallel_snapshot_objects to the canonical max_parallel_snapshot_tables:
// both names parse and resolve, and conflicting values are rejected.
func TestSalesforceMaxParallelSnapshotFieldNames(t *testing.T) {
	spec := salesforceCDCInputConfigSpec()
	const base = `
topics: [ Account ]
checkpoint_cache: my_cache
org_url: https://example.my.salesforce.com
client_id: my_id
client_secret: my_secret
`
	resolve := func(t *testing.T, yaml string) (int, error) {
		t.Helper()
		conf, err := spec.ParseYAML(base+yaml, nil)
		require.NoError(t, err)
		return cdcfield.ResolveInt(conf, sfciFieldMaxParallelSnapshotTables, sfciFieldMaxParallelSnapshotObjs, 1)
	}

	t.Run("deprecated name", func(t *testing.T) {
		v, err := resolve(t, `max_parallel_snapshot_objects: 4`)
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
		v, err := resolve(t, "max_parallel_snapshot_objects: 4\nmax_parallel_snapshot_tables: 4")
		require.NoError(t, err)
		assert.Equal(t, 4, v)
	})
	t.Run("both set to conflicting values", func(t *testing.T) {
		_, err := resolve(t, "max_parallel_snapshot_objects: 4\nmax_parallel_snapshot_tables: 8")
		require.Error(t, err)
	})
}
