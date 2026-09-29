// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
)

// TestIntegrationCheckpointCacheNameIsAView makes sure that the cache fails at start up when the configured table name
// belongs to an object that is not a table. CREATE TABLE fails with ORA-00955, the same error as when another pipeline
// created the table first, so the cache must not mistake the view for that table.
func TestIntegrationCheckpointCacheNameIsAView(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	viewName := db.Schema + ".CDC_CHECKPOINT_VIEW"
	db.MustExecContext(t.Context(),
		"CREATE VIEW "+viewName+" AS SELECT 'k' AS cache_key, HEXTORAW('00') AS cache_val FROM dual")

	c, err := newCheckpointCache(t.Context(), connStr, viewName, "k", service.MockResources().Logger())
	require.ErrorContains(t, err, "ORA-00955")
	require.Nil(t, c)
}

// TestIntegrationCheckpointCacheConcurrentCreate makes sure that pipelines which create the same table at the same
// time all start. Only one CREATE TABLE succeeds. The others get ORA-00955 and must continue.
func TestIntegrationCheckpointCacheConcurrentCreate(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	tbl, err := validateCacheTableName(db.Schema + ".CDC_CHECKPOINT_RACE")
	require.NoError(t, err)

	cacheDB, err := sql.Open("oracle", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cacheDB.Close()) })

	const pipelines = 5
	var (
		wg      sync.WaitGroup
		created atomic.Int32
		errs    [pipelines]error
		start   = make(chan struct{})
	)
	for i := range pipelines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := createCacheTable(t.Context(), cacheDB, tbl, "k", service.MockResources().Logger())
			errs[i] = err
			if ok {
				created.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "call %d", i)
	}
	assert.Equal(t, int32(1), created.Load(), "expected exactly one call to create the table")
}
