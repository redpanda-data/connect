// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb"
	oracledbtest "github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// TestIntegrationMigrateCheckpointCache can be deleted once we're happy customers have migrated.
func TestIntegrationMigrateCheckpointCache(t *testing.T) {
	integration.CheckSkip(t)
	// Not parallel: TestIntegrationOracleDBCDCSnapshotAndStreaming also uses
	// C##RPCN, and it is parallel (see SetupCDBTestWithPDB).

	cdbConnStr, pdbDB, pdbName := oracledbtest.SetupCDBTestWithPDB(t)
	require.NoError(t, pdbDB.CreatePDBTableWithSupplementalLoggingIfNotExists(t.Context(), pdbDB.Schema+".mtfoo", "CREATE TABLE "+pdbDB.Schema+".mtfoo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

	cdbDB, err := sql.Open("oracle", cdbConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cdbDB.Close()) })
	require.NoError(t, cdbDB.PingContext(t.Context()))

	var (
		cacheTableName     string
		expectedSCN        []byte
		defaultCacheKeyOld = "max_scn"
		defaultCacheKeyNew = "oracledb_cdc"
	)
	t.Log("Pre-creating checkpoint cache table to simulate an existing deployment...")
	{
		cacheTableName = fmt.Sprintf("C##RPCN.CDC_CHECKPOINT_%s", strings.ToUpper(pdbName))
		_, err = cdbDB.ExecContext(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			cache_key VARCHAR2(10) NOT NULL PRIMARY KEY,
			cache_val RAW(8)
		)`, cacheTableName))
		require.NoError(t, err)

		// testSCNBytes simulates a valid checkpoint from a prior deployment
		var scn uint64
		require.NoError(t, cdbDB.QueryRowContext(t.Context(), `SELECT CURRENT_SCN FROM V$DATABASE`).Scan(&scn))
		expectedSCN = make([]byte, 8)
		binary.LittleEndian.PutUint64(expectedSCN, scn)
		_, err = cdbDB.ExecContext(t.Context(), fmt.Sprintf(`INSERT INTO %s (cache_key, cache_val) VALUES (:1, :2)`, cacheTableName), defaultCacheKeyOld, expectedSCN)
		require.NoError(t, err)
	}

	var batch oracledbtest.Batch

	t.Log("Launching component...")
	cfg := `
oracledb_cdc:
  connection_string: %s
  pdb_name: %s
  stream_snapshot: false
  max_parallel_snapshot_tables: 1
  snapshot_max_batch_size: 10
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["` + pdbDB.Schema + `.MTFOO"]
  batching:
    count: 500`

	stream := oracledbtest.StartPipeline(t, fmt.Sprintf(cfg, cdbConnStr, pdbName), batch.Consumer(t))

	// Poll until the migration renames default 'max_scn' to config checkpoint_cache_key value.
	// The rename must not roll the SCN back. The mined checkpoint can move it forward before
	// the poll sees the new key, so the pre-migration SCN is a lower bound.
	var actualSCN []byte
	q := fmt.Sprintf(`SELECT cache_val FROM %s WHERE cache_key = :1`, cacheTableName)
	assert.Eventually(t, func() bool {
		return cdbDB.QueryRowContext(t.Context(), q, "oracledb_cdc").Scan(&actualSCN) == nil
	}, time.Minute, time.Second, "expected migration to rename 'max_scn' to 'oracledb_cdc'")
	assert.GreaterOrEqual(t, binary.LittleEndian.Uint64(actualSCN), binary.LittleEndian.Uint64(expectedSCN),
		"expected migration to keep or advance the pre-migration SCN, never roll it back")

	t.Log("Verifying streaming changes...")
	{
		want := 1000
		_, err = pdbDB.Exec(`
BEGIN
	FOR i IN 1..1000 LOOP
		INSERT INTO ` + pdbDB.Schema + `.mtfoo (id) VALUES (DEFAULT);
	END LOOP;
	COMMIT;
END;`)
		require.NoError(t, err)

		oracledbtest.WaitForCount(t, batch.Count, want, time.Minute*1)
	}

	require.NoError(t, stream.StopWithin(time.Second*10))

	// assert cache state via connection after the stream has fully stopped.
	var (
		charLen   int
		colQuery  = `SELECT CHAR_LENGTH FROM all_tab_columns WHERE owner = 'C##RPCN' AND table_name = :1 AND column_name = 'CACHE_KEY'`
		tableName = fmt.Sprintf("CDC_CHECKPOINT_%s", strings.ToUpper(pdbName))
	)
	require.NoError(t, cdbDB.QueryRowContext(t.Context(), colQuery, tableName).Scan(&charLen))
	assert.Equal(t, 128, charLen, "expected cache_key VARCHAR2 limit to be 128")

	// assert checkpoint cache key is migrated
	var (
		cacheKeyCount int
		keyQuery      = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE cache_key = :1`, cacheTableName)
	)
	require.NoError(t, cdbDB.QueryRowContext(t.Context(), keyQuery, defaultCacheKeyNew).Scan(&cacheKeyCount))
	assert.Equalf(t, 1, cacheKeyCount, "expected cache key '%s' to exist in checkpoint cache", defaultCacheKeyNew)

	// assert scn stored in cache as progressed
	var (
		finalSCNBytes []byte
		query         = fmt.Sprintf(`SELECT cache_val FROM %s WHERE cache_key = :1`, cacheTableName)
	)
	require.NoError(t, cdbDB.QueryRowContext(t.Context(), query, defaultCacheKeyNew).Scan(&finalSCNBytes))
	finalSCN := binary.LittleEndian.Uint64(finalSCNBytes)
	initialSCN := binary.LittleEndian.Uint64(expectedSCN)
	assert.Greaterf(t, finalSCN, initialSCN, "expected final SCN (%d) to have advanced beyond initial SCN (%d)", finalSCN, initialSCN)
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
	cacheA := newCheckpointCache(t, connStr, tableA, "pipeline_a")
	cacheB := newCheckpointCache(t, connStr, tableB, "pipeline_b")

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
	require.NoError(t, oracledb.CreateUpsertStoredProc(t.Context(), db.DB, table))

	t.Log("And a user with the grants that older versions needed, so no INSERT on the table")
	userConnStr := db.CreateUserWithGrants(connStr, user,
		"GRANT SELECT, UPDATE ON "+table+" TO "+user,
		"GRANT EXECUTE ON "+db.Schema+".CDC_CHECKPOINT_CACHE_UPDATE TO "+user)

	t.Log("When the user writes a checkpoint")
	cache := newCheckpointCache(t, userConnStr, table, "fallback")
	require.NoError(t, cache.Set(t.Context(), "fallback", scnBytes(12345), nil))

	t.Log("Then the procedure writes it to the table")
	assert.Equal(t, map[string][]byte{"fallback": scnBytes(12345)}, cacheRows(t, db, table))
}

// TestIntegrationProbeCheckpointMerge makes sure that the probe reports a user
// that cannot write a checkpoint with MERGE, and that it leaves no row in the
// checkpoint table. MERGE runs with the rights of the user, and needs INSERT and
// UPDATE on the table.
func TestIntegrationProbeCheckpointMerge(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	table := db.Schema + ".CDC_CHECKPOINT_PROBE"
	db.MustExecContext(t.Context(), `CREATE TABLE `+table+` (cache_key VARCHAR2(128) NOT NULL PRIMARY KEY, cache_val RAW(8))`)

	tests := []struct {
		name       string
		grants     string
		wantDenied bool
	}{
		{
			name:       "no insert",
			grants:     "SELECT, UPDATE",
			wantDenied: true,
		},
		{
			name:       "select, insert and update",
			grants:     "SELECT, INSERT, UPDATE",
			wantDenied: false,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Given a user with %s on the checkpoint table", tc.grants)
			user := fmt.Sprintf("%s_P%d", db.Schema, i)
			userConnStr := db.CreateUserWithGrants(connStr, user, "GRANT "+tc.grants+" ON "+table+" TO "+user)
			userDB, err := sql.Open("oracle", userConnStr)
			require.NoError(t, err)
			// Registered after the user, so it runs before the drop of the user.
			t.Cleanup(func() { assert.NoError(t, userDB.Close()) })

			t.Log("When the probe runs as that user")
			err = oracledb.ProbeCheckpointMerge(t.Context(), userDB, table)

			t.Log("Then it reports a missing privilege only without INSERT, and leaves no row")
			if tc.wantDenied {
				_, missing := replication.IsMissingPrivilege(err)
				assert.Truef(t, missing, "expected a missing privilege error, got: %v", err)
			} else {
				assert.NoError(t, err)
			}
			assert.Empty(t, cacheRows(t, db, table))
		})
	}
}

// newCheckpointCache returns a checkpoint cache for table and closes it when
// the test ends. Call it after CreateUserWithGrants, so that the cache closes
// its connections before the user is dropped.
func newCheckpointCache(t *testing.T, connStr, table, key string) service.Cache {
	t.Helper()
	cache, err := oracledb.NewCheckpointCache(t.Context(), connStr, table, key)
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
