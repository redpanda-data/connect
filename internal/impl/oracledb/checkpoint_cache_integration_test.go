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
	"errors"
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
	oracledbtest "github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// TestIntegrationMigrateCheckpointCache can be deleted once we're happy customers have migrated.
func TestIntegrationMigrateCheckpointCache(t *testing.T) {
	integration.CheckSkip(t)

	cdbConnStr, pdbDB, pdbName := oracledbtest.SetupCDBTestWithPDB(t)
	require.NoError(t, pdbDB.CreatePDBTableWithSupplementalLoggingIfNotExists(t.Context(), "testdb.mtfoo", "CREATE TABLE testdb.mtfoo (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"))

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
	t.Log("Pre-creating checkpoint cache table in to simulate an existing deployment...")
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
    backoff_interval: 1s
  include: ["TESTDB.MTFOO"]
  batching:
    count: 500`

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, cdbConnStr, pdbName)))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		batch.Lock()
		defer batch.Unlock()
		for _, msg := range mb {
			msgBytes, err := msg.AsBytes()
			assert.NoError(t, err)
			batch.Msgs = append(batch.Msgs, string(msgBytes))
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	// Poll until the migration renames default 'max_scn' to config checkpoint_cache_key value, then immediately
	// assert the SCN value is unchanged — the migration must only rename the key.
	var actualSCN []byte
	q := fmt.Sprintf(`SELECT cache_val FROM %s WHERE cache_key = :1`, cacheTableName)
	assert.Eventually(t, func() bool {
		return cdbDB.QueryRowContext(t.Context(), q, "oracledb_cdc").Scan(&actualSCN) == nil
	}, time.Minute, time.Second, "expected migration to rename 'max_scn' to 'oracledb_cdc'")
	assert.Equal(t, expectedSCN, actualSCN, "expected migration to leave SCN value unchanged")

	t.Log("Verifying streaming changes...")
	{
		want := 1000
		_, err = pdbDB.Exec(`
BEGIN
	FOR i IN 1..1000 LOOP
		INSERT INTO testdb.mtfoo (id) VALUES (DEFAULT);
	END LOOP;
	COMMIT;
END;`)
		require.NoError(t, err)

		var got int
		assert.Eventually(t, func() bool {
			got = batch.Count()
			return got >= want
		}, time.Minute*1, time.Second*1)
		assert.Equalf(t, want, got, "Wanted %d streaming messages but got %d", want, got)
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
