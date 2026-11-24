// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"database/sql"
	"encoding/binary"
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

	c, err := newCheckpointCache(t.Context(), connStr, viewName, "k", service.MockResources().Metrics(), service.MockResources().Logger())
	require.ErrorContains(t, err, "ORA-00955")
	require.ErrorContains(t, err, "not a table visible to this user")
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
		wg.Go(func() {
			<-start
			ok, err := createCacheTable(t.Context(), cacheDB, tbl, "k", service.MockResources().Logger())
			errs[i] = err
			if ok {
				created.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "call %d", i)
	}
	assert.Equal(t, int32(1), created.Load(), "expected exactly one call to create the table")
}

// TestIntegrationCheckpointCacheTablesInSameSchema makes sure that two caches
// with two tables in one schema each write to their own table. Older versions
// wrote through one procedure per schema, whose body named the table of the
// first cache. The second cache wrote to that table and read from its own, so
// after a restart it found no checkpoint. Multitenant mode always has this
// layout: one table per PDB under C##RPCN.
func TestIntegrationCheckpointCacheTablesInSameSchema(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	tableA := db.Schema + ".CDC_CHECKPOINT_A"
	tableB := db.Schema + ".CDC_CHECKPOINT_B"

	t.Log("Given two checkpoint caches with two tables in one schema")
	cacheA := newTestCheckpointCache(t, connStr, tableA, "pipeline_a")
	cacheB := newTestCheckpointCache(t, connStr, tableB, "pipeline_b")

	t.Log("When each cache writes its checkpoint")
	require.NoError(t, cacheA.Set(t.Context(), "pipeline_a", scnBytes(1), nil))
	require.NoError(t, cacheB.Set(t.Context(), "pipeline_b", scnBytes(2), nil))

	t.Log("Then each checkpoint is in the table of its own cache only")
	assert.Equal(t, map[string][]byte{"pipeline_a": scnBytes(1)}, cacheRows(t, db, tableA))
	assert.Equal(t, map[string][]byte{"pipeline_b": scnBytes(2)}, cacheRows(t, db, tableB))
}

// TestIntegrationCheckpointCacheProcedureFallback makes sure that a user without
// INSERT on the checkpoint table can still write checkpoints. Older versions
// wrote them with a stored procedure that runs with the rights of its owner. So
// a DBA could grant only SELECT and UPDATE on the table and EXECUTE on the
// procedure. Such a user must keep working after an upgrade.
func TestIntegrationCheckpointCacheProcedureFallback(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	table := db.Schema + ".CDC_CHECKPOINT_FALLBACK"
	user := db.Schema + "_F"

	t.Log("Given a checkpoint table and the procedure that older versions created")
	db.MustExecContext(t.Context(), `CREATE TABLE `+table+` (cache_key VARCHAR2(128) NOT NULL PRIMARY KEY, cache_val RAW(8))`)
	require.NoError(t, createUpsertStoredProc(t.Context(), db.DB, mustCacheTable(t, table)))

	t.Log("And a user with the grants that older versions needed, so no INSERT on the table")
	userConnStr := db.CreateUserWithGrants(connStr, user,
		"GRANT SELECT, UPDATE ON "+table+" TO "+user,
		"GRANT EXECUTE ON "+db.Schema+".CDC_CHECKPOINT_CACHE_UPDATE TO "+user)

	t.Log("When the user writes a checkpoint")
	cache := newTestCheckpointCache(t, userConnStr, table, "fallback")
	require.NoError(t, cache.Set(t.Context(), "fallback", scnBytes(12345), nil))

	t.Log("Then the procedure writes it to the table")
	assert.Equal(t, map[string][]byte{"fallback": scnBytes(12345)}, cacheRows(t, db, table))
}

// newTestCheckpointCache returns a checkpoint cache for table and closes it when
// the test ends. Call it after CreateUserWithGrants, so that the cache closes
// its connections before the user is dropped.
func newTestCheckpointCache(t *testing.T, connStr, table, key string) *checkpointCache {
	t.Helper()
	cache, err := newCheckpointCache(t.Context(), connStr, table, key, service.MockResources().Metrics(), service.MockResources().Logger())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cache.Close(context.Background())) })
	return cache
}

// cacheRows returns the rows of the checkpoint table, by cache key.
func cacheRows(t *testing.T, db *oracledbtest.TestDB, table string) map[string][]byte {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT cache_key, cache_val FROM "+table)
	require.NoError(t, err)
	defer rows.Close()
	got := map[string][]byte{}
	for rows.Next() {
		var (
			key string
			val []byte
		)
		require.NoError(t, rows.Scan(&key, &val))
		got[key] = val
	}
	require.NoError(t, rows.Err())
	return got
}

// scnBytes returns scn encoded as the checkpoint cache stores it.
func scnBytes(scn uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, scn)
	return b
}

// mustCacheTable parses table as the checkpoint_cache_table_name field does.
func mustCacheTable(t *testing.T, table string) cacheTable {
	t.Helper()
	tbl, err := validateCacheTableName(table)
	require.NoError(t, err)
	return tbl
}
