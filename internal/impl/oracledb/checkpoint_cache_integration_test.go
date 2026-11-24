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
	"net/url"
	"strings"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
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

// TestIntegrationCheckpointCacheTablesInSameSchema makes sure that each pipeline writes its checkpoint to its own
// checkpoint_cache_table_name when two pipelines use two tables in the same schema.
func TestIntegrationCheckpointCacheTablesInSameSchema(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), db.Schema+".cpfoo",
		"CREATE TABLE "+db.Schema+".cpfoo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

	tableA := db.Schema + ".CDC_CHECKPOINT_A"
	tableB := db.Schema + ".CDC_CHECKPOINT_B"

	queryKeyCount := func(table, key string) (int, error) {
		var n int
		err := db.QueryRowContext(t.Context(), fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE cache_key = :1`, table), key).Scan(&n)
		return n, err
	}
	countKey := func(table, key string) int {
		n, err := queryKeyCount(table, key)
		require.NoError(t, err)
		return n
	}

	// runPipeline starts a pipeline, inserts rows until a checkpoint for the key is in one of pollTables, then stops
	// the pipeline. Pipeline B also polls the table of pipeline A: with the bug, its checkpoint lands there, and the
	// test then fails on the assertion instead of the timeout.
	runPipeline := func(table, key string, pollTables ...string) {
		cfg := `
oracledb_cdc:
  connection_string: ` + connStr + `
  stream_snapshot: false
  checkpoint_cache_table_name: ` + table + `
  checkpoint_cache_key: ` + key + `
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["` + db.Schema + `.CPFOO"]`

		var batch oracledbtest.MsgBatch
		stream := oracledbtest.StartPipeline(t, cfg, batch.Consumer())

		require.Eventually(t, func() bool {
			// No require in here: Eventually runs the condition on another goroutine.
			_, err := db.ExecContext(t.Context(), "INSERT INTO "+db.Schema+".cpfoo (id) VALUES (DEFAULT)")
			assert.NoError(t, err)
			// The pipeline creates its table at start up, so ignore query errors until the table exists.
			for _, pt := range pollTables {
				if n, err := queryKeyCount(pt, key); err == nil && n > 0 {
					return true
				}
			}
			return false
		}, time.Minute, time.Second, "expected a checkpoint for key '%s'", key)

		require.NoError(t, stream.StopWithin(10*time.Second))
	}

	t.Log("Running pipeline A...")
	runPipeline(tableA, "pipeline_a", tableA)
	assert.Equal(t, 1, countKey(tableA, "pipeline_a"), "expected checkpoint of pipeline A in its own table")

	t.Log("Running pipeline B...")
	runPipeline(tableB, "pipeline_b", tableA, tableB)
	assert.Equal(t, 1, countKey(tableB, "pipeline_b"), "expected checkpoint of pipeline B in its own table")
	assert.Equal(t, 0, countKey(tableA, "pipeline_b"), "expected no checkpoint of pipeline B in the table of pipeline A")
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
	proc := db.Schema + ".CDC_CHECKPOINT_CACHE_UPDATE"
	db.MustExecContext(t.Context(), `CREATE TABLE `+table+` (cache_key VARCHAR2(128) NOT NULL PRIMARY KEY, cache_val RAW(8))`)
	// The procedure that older versions created.
	db.MustExecContext(t.Context(), `
		CREATE PROCEDURE `+proc+` (
			p_key IN VARCHAR2,
			p_value IN RAW
		)
		AS
			v_count NUMBER;
		BEGIN
			SELECT COUNT(*) INTO v_count FROM `+table+` WHERE cache_key = p_key;

			IF v_count > 0 THEN
				UPDATE `+table+` SET cache_val = p_value WHERE cache_key = p_key;
			ELSE
				INSERT INTO `+table+` (cache_key, cache_val) VALUES (p_key, p_value);
			END IF;

			COMMIT;
		END;`)

	user := db.Schema + "_F"
	userConnStr := createUserWithGrants(t, db, connStr, user,
		"GRANT SELECT, UPDATE ON "+table+" TO "+user,
		"GRANT EXECUTE ON "+proc+" TO "+user)

	cache, err := oracledb.NewCheckpointCache(t.Context(), userConnStr, table, "fallback")
	require.NoError(t, err)
	// Registered after the drop of the user, so it runs first.
	t.Cleanup(func() { assert.NoError(t, cache.Close(context.Background())) })

	want := make([]byte, 8)
	binary.LittleEndian.PutUint64(want, 12345)
	require.NoError(t, cache.Set(t.Context(), "fallback", want, nil))

	var got []byte
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT cache_val FROM "+table+" WHERE cache_key = :1", "fallback").Scan(&got))
	assert.Equal(t, want, got)
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
			name:       "select and update",
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
			user := fmt.Sprintf("%s_P%d", db.Schema, i)
			userConnStr := createUserWithGrants(t, db, connStr, user, "GRANT "+tc.grants+" ON "+table+" TO "+user)
			userDB, err := sql.Open("oracle", userConnStr)
			require.NoError(t, err)
			// Registered after the drop of the user, so it runs first: Oracle
			// cannot drop a user that is connected (ORA-01940).
			t.Cleanup(func() { assert.NoError(t, userDB.Close()) })

			err = oracledb.ProbeCheckpointMerge(t.Context(), userDB, table)
			if tc.wantDenied {
				require.Error(t, err)
				_, ok := replication.IsMissingPrivilege(err)
				assert.Truef(t, ok, "expected a missing privilege error, got: %v", err)
			} else {
				require.NoError(t, err)
			}

			var rows int
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&rows))
			assert.Zero(t, rows, "expected the probe to roll back its row")
		})
	}
}

// createUserWithGrants creates a user with CREATE SESSION and the grants, and
// returns a connection string for it. It drops the user when the test ends, so
// close the connections of the user in a cleanup that is registered later.
func createUserWithGrants(t *testing.T, db *oracledbtest.TestDB, connStr, user string, grants ...string) string {
	t.Helper()
	ctx := t.Context()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	// Local users in CDB$ROOT need _ORACLE_SCRIPT (to avoid ORA-65096). It is a
	// session setting.
	_, err = conn.ExecContext(ctx, `ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE`)
	require.NoError(t, err)
	for _, q := range append([]string{"CREATE USER " + user + " IDENTIFIED BY probe123", "GRANT CREATE SESSION TO " + user}, grants...) {
		_, err = conn.ExecContext(ctx, q)
		require.NoErrorf(t, err, "creating user %s: %s", user, q)
	}
	t.Cleanup(func() {
		// t.Context() is cancelled before cleanup runs.
		ctx := context.Background()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Errorf("dropping user %s: %v", user, err)
			return
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE`); err != nil {
			t.Errorf("dropping user %s: %v", user, err)
			return
		}
		if _, err := conn.ExecContext(ctx, "DROP USER "+user+" CASCADE"); err != nil {
			t.Errorf("dropping user %s: %v", user, err)
		}
	})

	u, err := url.Parse(connStr)
	require.NoError(t, err)
	u.User = url.UserPassword(user, "probe123")
	return u.String()
}
