// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"database/sql"
	"log/slog"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

func TestProcessRedoEventWithInMemoryCache(t *testing.T) {
	t.Run("single transaction commit", func(t *testing.T) {
		cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})

		require.NoError(t, err)
		require.Len(t, pub.messages, 1)
		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN)
	})

	// When transaction A commits while transaction B is still open, the checkpoint
	// must not advance past B's start SCN - 1. If it did, a restart would begin
	// mining at A's commit SCN and miss B's already-seen DML events — silently
	// losing B's changes when its COMMIT is later encountered.
	t.Run("concurrent transactions commit", func(t *testing.T) {
		cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910)
			txACommit = uint64(1000)
			txBCommit = uint64(1050)
		)

		// Seed both transactions. B remains open when A commits.
		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txB", txBStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		// Commit tranaction A, transaction B still open.
		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		msg := "while B is open, CheckpointSCN must be held back to B.startSCN-1 to avoid skipping transaction B on restart"
		assert.Equal(t, replication.SCN(txBStart-1), pub.messages[0].CheckpointSCN, msg)

		// Commit B — no open transactions remain.
		err = lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txBCommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txB",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 2, "B's commit must publish its events")

		msg = "with no remaining open transactions, CheckpointSCN must equal B's commit SCN"
		assert.Equal(t, replication.SCN(txBCommit), pub.messages[1].CheckpointSCN, msg)
	})

	// A transaction that receives OpStart but no DML events (e.g. a read-only
	// or DDL transaction on an unsubscribed table) must not hold back the
	// checkpoint watermark — it has nothing to replay on restart.
	t.Run("open transaction with no events does not hold back checkpoint", func(t *testing.T) {
		cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910) // starts but never gets DML events
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		// txB is started but never receives any DML events
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		msg := "txB has no DML events so it must not hold back the checkpoint"
		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN, msg)
	})
}

func TestProcessRedoEventWithConnectCacheResource(t *testing.T) {
	newCacheResource := func(t *testing.T) *ConnectCacheResource {
		t.Helper()
		res := service.MockResources(service.MockResourcesOptAddCache("txn_cache"))
		cfg := TransactionCacheConfig{CacheName: "txn_cache", CacheKey: "oracledb_cdc", MaxEvents: 0}
		return NewConnectCacheResource(res, cfg, res.Metrics(), service.NewLoggerFromSlog(slog.Default()))
	}

	t.Run("single transaction commit", func(t *testing.T) {
		cache := newCacheResource(t)
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})

		require.NoError(t, err)
		require.Len(t, pub.messages, 1)
		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN)
	})

	// When transaction A commits while transaction B is still open, the checkpoint
	// must not advance past B's start SCN - 1, otherwise a restart would skip
	// B's already-seen DML events.
	t.Run("concurrent transactions checkpoint held back to lowest open SCN", func(t *testing.T) {
		cache := newCacheResource(t)
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910)
			txACommit = uint64(1000)
			txBCommit = uint64(1050)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txB", txBStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		msg := "while B is open, CheckpointSCN must be held back to B.startSCN-1 to avoid skipping transaction B on restart"
		assert.Equal(t, replication.SCN(txBStart-1), pub.messages[0].CheckpointSCN, msg)

		err = lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txBCommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txB",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 2, "B's commit must publish its events")

		msg = "with no remaining open transactions, CheckpointSCN must equal B's commit SCN"
		assert.Equal(t, replication.SCN(txBCommit), pub.messages[1].CheckpointSCN, msg)
	})

	// A transaction that receives OpStart but no DML events (e.g. a read-only
	// or DDL transaction on an unsubscribed table) must not hold back the
	// checkpoint watermark — it has nothing to replay on restart.
	t.Run("open transaction with no events does not hold back checkpoint", func(t *testing.T) {
		cache := newCacheResource(t)
		pub := &publisherStub{}
		lm := newLogMiner(pub, cache)

		const (
			txAStart  = uint64(900)
			txBStart  = uint64(910) // starts but never gets DML events
			txACommit = uint64(1000)
		)

		require.NoError(t, cache.StartTransaction(t.Context(), "txA", txAStart))
		require.NoError(t, cache.AddEvent(t.Context(), "txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
		// txB is started but never receives any DML events
		require.NoError(t, cache.StartTransaction(t.Context(), "txB", txBStart))

		err := lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           txACommit,
			Operation:     sqlredo.OpCommit,
			TransactionID: "txA",
		})
		require.NoError(t, err)
		require.Len(t, pub.messages, 1, "A's commit must publish its events")

		assert.Equal(t, replication.SCN(txACommit), pub.messages[0].CheckpointSCN,
			"txB has no DML events so it must not hold back the checkpoint")
	})
}

func TestSessionManagerFilesChanged(t *testing.T) {
	tests := []struct {
		name        string
		loaded      []*LogFile
		incoming    []*LogFile
		wantChanged bool
	}{
		{
			name:        "no files loaded yet",
			loaded:      nil,
			incoming:    []*LogFile{{FileName: "redo01.log"}},
			wantChanged: true,
		},
		{
			name:        "same files in same order reports unchanged",
			loaded:      []*LogFile{{FileName: "redo01.log"}, {FileName: "redo02.log"}},
			incoming:    []*LogFile{{FileName: "redo01.log"}, {FileName: "redo02.log"}},
			wantChanged: false,
		},
		{
			name:        "more files incoming than loaded",
			loaded:      []*LogFile{{FileName: "redo01.log"}},
			incoming:    []*LogFile{{FileName: "redo01.log"}, {FileName: "redo02.log"}},
			wantChanged: true,
		},
		{
			name:        "fewer files incoming than loaded",
			loaded:      []*LogFile{{FileName: "redo01.log"}, {FileName: "redo02.log"}},
			incoming:    []*LogFile{{FileName: "redo01.log"}},
			wantChanged: true,
		},
		{
			name:        "same count but different filename",
			loaded:      []*LogFile{{FileName: "redo01.log"}},
			incoming:    []*LogFile{{FileName: "redo02.log"}},
			wantChanged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SessionManager{loadedFiles: tt.loaded}
			assert.Equal(t, tt.wantChanged, sm.logFilesChanged(tt.incoming))
		})
	}
}

func TestShouldDeferMiningCycle(t *testing.T) {
	tests := []struct {
		name          string
		currentSCN    uint64
		dbSCN         uint64
		minWindowSize int
		shouldDefer   bool
	}{
		{
			name:          "gap smaller than min window defers the cycle",
			currentSCN:    1000,
			dbSCN:         1005,
			minWindowSize: 100,
			shouldDefer:   true,
		},
		{
			name:          "gap equal to min window does not defer",
			currentSCN:    1000,
			dbSCN:         1100,
			minWindowSize: 100,
			shouldDefer:   false,
		},
		{
			name:          "gap larger than min window does not defer",
			currentSCN:    1000,
			dbSCN:         2000,
			minWindowSize: 100,
			shouldDefer:   false,
		},
		{
			name:          "min window of zero disables the guard",
			currentSCN:    1000,
			dbSCN:         1001,
			minWindowSize: 0,
			shouldDefer:   false,
		},
		{
			name:          "scns equal does not defer (existing guard handles this)",
			currentSCN:    1000,
			dbSCN:         1000,
			minWindowSize: 100,
			shouldDefer:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deferMiningCycle(tt.currentSCN, tt.dbSCN, tt.minWindowSize)
			assert.Equal(t, tt.shouldDefer, got)
		})
	}
}

// TestBasicfileOORInferFromLOBOnlyUpdate covers the exact CI failure scenario where:
//   - A BASICFILE DISABLE STORAGE IN ROW LOB column (OOL_COL) has its LOB_WRITE events
//     arrive before any DML event; all are deferred.
//   - Oracle emits a LOB-only UPDATE for a SecureFile column (SECUREFILE_COL) that does
//     NOT include OOL_COL in its SET clause (BASICFILE OOR columns are absent).
//   - There is no INSERT event in the transaction cache.
//   - At COMMIT time, inferLOBLocator must treat OOL_COL as a BASICFILE OOR candidate
//     from the LOB-only UPDATE (absent from SET = BASICFILE OOR, not a disqualifier).
func TestBasicfileOORInferFromLOBOnlyUpdate(t *testing.T) {
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	pub := &publisherStub{}
	lm := newLogMiner(pub, cache)
	lm.cfg.LOBEnabled = true
	lm.lobColTypes = map[string]string{
		"TESTDB.T.OOL_COL":        "CLOB",
		"TESTDB.T.SECUREFILE_COL": "CLOB",
	}

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 100, Operation: sqlredo.OpStart, TransactionID: "txA",
	}))

	// OOL_COL LOB_WRITEs arrive with no DML events in txnCache yet — all deferred.
	for _, write := range []string{
		" buf_c := 'hello';\n  dbms_lob.write(loc_c, 5, 1, buf_c);",
		" buf_c := 'world';\n  dbms_lob.write(loc_c, 5, 6, buf_c);",
	} {
		require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
			SCN:           101,
			Operation:     sqlredo.OpLobWrite,
			TransactionID: "txA",
			SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
			TableName:     sql.NullString{String: "T", Valid: true},
			SQLRedo:       sql.NullString{String: write, Valid: true},
		}))
	}
	assert.Len(t, lm.pendingLOBWrites["txA"], 2, "OOL_COL LOB_WRITEs should be deferred")

	// Oracle emits a LOB-only UPDATE for SECUREFILE_COL. OOL_COL (BASICFILE OOR) is
	// absent from the SET clause — this is the case that triggered the CI failure.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           102,
		Operation:     sqlredo.OpUpdate,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `update "TESTDB"."T" set "SECUREFILE_COL" = 'X' where "ID" = '42'`, Valid: true},
	}))

	// SECUREFILE_COL gets its real data via SELECT_LOB_LOCATOR + LOB_WRITE.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           103,
		Operation:     sqlredo.OpSelectLobLocator,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `declare lob_1 clob; begin select "SECUREFILE_COL" into lob_1 from "TESTDB"."T" where "ID" = '42';`, Valid: true},
	}))
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           104,
		Operation:     sqlredo.OpLobWrite,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: " buf_c := 'securedata';\n  dbms_lob.write(loc_c, 10, 1, buf_c);", Valid: true},
	}))

	// COMMIT — replay deferred LOB_WRITEs; inferLOBLocator must find OOL_COL as a
	// candidate from the LOB-only UPDATE (absent from SET = BASICFILE OOR, not a skip).
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 200, Operation: sqlredo.OpCommit, TransactionID: "txA",
	}))

	require.Len(t, pub.messages, 1)
	data, ok := pub.messages[0].Data.(map[string]any)
	require.True(t, ok, "Data should be map[string]any")
	assert.Equal(t, "helloworld", data["OOL_COL"], "BASICFILE OOR column should have deferred LOB_WRITEs assembled")
	assert.Equal(t, "securedata", data["SECUREFILE_COL"], "SecureFile column should have its LOB_WRITE data")
	assert.Empty(t, lm.pendingLOBWrites, "no LOB_WRITEs should remain deferred after COMMIT")
}

// TestLOBOnlyUpdateSuppressionIsPerRowNotPerTable verifies that a LOB-only UPDATE is
// only suppressed when ITS OWN row was actually merged into a matching INSERT, not
// merely because some other row in the same table had an INSERT in the transaction.
func TestLOBOnlyUpdateSuppressionIsPerRowNotPerTable(t *testing.T) {
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	pub := &publisherStub{}
	lm := newLogMiner(pub, cache)
	lm.cfg.LOBEnabled = true
	lm.lobColTypes = map[string]string{"TESTDB.T.DESC": "CLOB"}

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 100, Operation: sqlredo.OpStart, TransactionID: "txA",
	}))

	// Row 1: INSERT (LOB column omitted, as Oracle does) followed by its inline
	// LOB-init UPDATE - merges into the INSERT and is suppressed.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           101,
		Operation:     sqlredo.OpInsert,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `insert into "TESTDB"."T" ("ID","NAME") values ('1','foo')`, Valid: true},
	}))
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           102,
		Operation:     sqlredo.OpUpdate,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `update "TESTDB"."T" set "DESC" = 'row1 desc' where "ID" = '1'`, Valid: true},
	}))

	// Row 2: a LOB-only UPDATE with no matching INSERT anywhere in the transaction,
	// despite being in the same table as row 1's INSERT.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           103,
		Operation:     sqlredo.OpUpdate,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `update "TESTDB"."T" set "DESC" = 'row2 desc' where "ID" = '2'`, Valid: true},
	}))

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 200, Operation: sqlredo.OpCommit, TransactionID: "txA",
	}))

	require.Len(t, pub.messages, 2, "row 1's merged INSERT and row 2's unmerged UPDATE must both be published")

	row1 := pub.messages[0].Data.(map[string]any)
	assert.Equal(t, "row1 desc", row1["DESC"], "row 1's LOB-only UPDATE should be merged into its INSERT")

	row2 := pub.messages[1].Data.(map[string]any)
	assert.Equal(t, "2", row2["ID"], "row 2's unmerged LOB-only UPDATE should be published as-is")
	assert.Equal(t, "row2 desc", row2["DESC"])
}

// TestLOBOnlyUpdateSuppressionFallsBackToTableLevelWhenLOBDisabled verifies that with
// LOBEnabled=false, Oracle's internal LOB-initialisation UPDATEs are still suppressed
// via the table-level check, since no per-row merge is ever attempted in that mode -
// mergedIntoInsert is only populated when LOBEnabled is true, so gating suppression on
// it alone would leave every LOB-init UPDATE unsuppressed and published as a spurious
// extra event whenever LOB support is disabled.
func TestLOBOnlyUpdateSuppressionFallsBackToTableLevelWhenLOBDisabled(t *testing.T) {
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	pub := &publisherStub{}
	lm := newLogMiner(pub, cache)
	lm.cfg.LOBEnabled = false
	lm.lobColTypes = map[string]string{"TESTDB.T.DESC": "CLOB"}

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 100, Operation: sqlredo.OpStart, TransactionID: "txA",
	}))

	// Row 1: INSERT followed by its inline LOB-init UPDATE. No merge is attempted
	// with LOBEnabled=false, but the UPDATE must still be suppressed.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           101,
		Operation:     sqlredo.OpInsert,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `insert into "TESTDB"."T" ("ID","NAME") values ('1','foo')`, Valid: true},
	}))
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           102,
		Operation:     sqlredo.OpUpdate,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `update "TESTDB"."T" set "DESC" = 'row1 desc' where "ID" = '1'`, Valid: true},
	}))

	// Row 2: a LOB-only UPDATE with no INSERT of its own, but the table still has row
	// 1's INSERT. The table-level fallback suppresses this too - unlike LOBEnabled=true,
	// there is no per-row merge outcome here that could be lost by suppressing it.
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           103,
		Operation:     sqlredo.OpUpdate,
		TransactionID: "txA",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `update "TESTDB"."T" set "DESC" = 'row2 desc' where "ID" = '2'`, Valid: true},
	}))

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 200, Operation: sqlredo.OpCommit, TransactionID: "txA",
	}))

	require.Len(t, pub.messages, 1, "both LOB-only UPDATEs must be suppressed, leaving only row 1's INSERT")

	row1 := pub.messages[0].Data.(map[string]any)
	assert.NotContains(t, row1, "DESC", "LOBEnabled=false must not merge or leak LOB column data")
}

func TestLowWatermarkSCN(t *testing.T) {
	tests := []struct {
		name         string
		excludeTxnID sqlredo.TransactionID
		// dmlTxns gives the start SCN of each open transaction. Each one has one DML event.
		dmlTxns map[sqlredo.TransactionID]uint64
		// lobTxns gives the start SCN of each open transaction that has no DML events
		// and is marked with IncludeInLowWatermark.
		lobTxns map[sqlredo.TransactionID]uint64
		want    uint64
	}{
		{
			name: "no open state",
			want: math.MaxUint64,
		},
		{
			name:    "lowest of DML and LOB transactions",
			dmlTxns: map[sqlredo.TransactionID]uint64{"txB": 900},
			lobTxns: map[sqlredo.TransactionID]uint64{"txC": 800, "txD": 850},
			want:    800,
		},
		{
			name:         "committing transaction is excluded",
			excludeTxnID: "txA",
			dmlTxns:      map[sqlredo.TransactionID]uint64{"txA": 700},
			lobTxns:      map[sqlredo.TransactionID]uint64{"txA": 710, "txB": 900},
			want:         900,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
			for txnID, scn := range tt.dmlTxns {
				require.NoError(t, cache.StartTransaction(t.Context(), txnID, scn))
				require.NoError(t, cache.AddEvent(t.Context(), txnID, scn, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}))
			}
			for txnID, scn := range tt.lobTxns {
				require.NoError(t, cache.StartTransaction(t.Context(), txnID, scn))
				require.NoError(t, cache.IncludeInLowWatermark(t.Context(), txnID))
			}

			assert.Equal(t, tt.want, cache.LowWatermarkSCN(tt.excludeTxnID))
		})
	}
}

// TestCommitCheckpointStaysBelowOpenLOBTransaction verifies that when A commits
// while LOB-only transaction B is open, A's checkpoint stays below B's START, or
// below B's first LOB event if the START is unknown.
func TestCommitCheckpointStaysBelowOpenLOBTransaction(t *testing.T) {
	tests := []struct {
		name      string
		omitStart bool
		want      replication.SCN
		txBEvents []*sqlredo.RedoEvent
	}{
		{
			name: "SecureFile locator and write",
			want: 99,
			txBEvents: []*sqlredo.RedoEvent{
				{
					SCN:           110,
					Operation:     sqlredo.OpSelectLobLocator,
					TransactionID: "txB",
					SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
					TableName:     sql.NullString{String: "T", Valid: true},
					SQLRedo:       sql.NullString{String: `declare lob_1 clob; begin select "DOC" into lob_1 from "TESTDB"."T" where "ID" = '42';`, Valid: true},
				},
				{
					SCN:           111,
					Operation:     sqlredo.OpLobWrite,
					TransactionID: "txB",
					SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
					TableName:     sql.NullString{String: "T", Valid: true},
					SQLRedo:       sql.NullString{String: " buf_c := 'hello';\n  dbms_lob.write(loc_c, 5, 1, buf_c);", Valid: true},
				},
			},
		},
		{
			name: "deferred write only",
			want: 99,
			txBEvents: []*sqlredo.RedoEvent{
				{
					SCN:           110,
					Operation:     sqlredo.OpLobWrite,
					TransactionID: "txB",
					SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
					TableName:     sql.NullString{String: "T", Valid: true},
					SQLRedo:       sql.NullString{String: " buf_c := 'hello';\n  dbms_lob.write(loc_c, 5, 1, buf_c);", Valid: true},
				},
			},
		},
		{
			name:      "START unknown stays below first LOB event",
			omitStart: true,
			want:      109,
			txBEvents: []*sqlredo.RedoEvent{
				{
					SCN:           110,
					Operation:     sqlredo.OpLobWrite,
					TransactionID: "txB",
					SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
					TableName:     sql.NullString{String: "T", Valid: true},
					SQLRedo:       sql.NullString{String: " buf_c := 'hello';\n  dbms_lob.write(loc_c, 5, 1, buf_c);", Valid: true},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
			pub := &publisherStub{}
			lm := newLogMiner(pub, cache)
			lm.cfg.LOBEnabled = true
			lm.lobColTypes = map[string]string{"TESTDB.T.DOC": "CLOB"}

			if !tt.omitStart {
				require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
					SCN: 100, Operation: sqlredo.OpStart, TransactionID: "txB",
				}))
			}
			for _, ev := range tt.txBEvents {
				require.NoError(t, lm.processRedoEvent(t.Context(), ev))
			}
			require.NotEmpty(t, len(lm.lobStates)+len(lm.pendingLOBWrites), "txB must hold LOB state")

			require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
				SCN: 120, Operation: sqlredo.OpStart, TransactionID: "txA",
			}))
			require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
				SCN:           121,
				Operation:     sqlredo.OpInsert,
				TransactionID: "txA",
				SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
				TableName:     sql.NullString{String: "U", Valid: true},
				SQLRedo:       sql.NullString{String: `insert into "TESTDB"."U" ("ID") values ('1')`, Valid: true},
			}))
			require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
				SCN: 130, Operation: sqlredo.OpCommit, TransactionID: "txA",
			}))

			require.Len(t, pub.messages, 1)
			assert.Equal(t, tt.want, pub.messages[0].CheckpointSCN, "the checkpoint must stay below the START of txB (or its first LOB event if START is unknown)")
		})
	}
}

// TestLOBTransactionReleasesCheckpointOnEnd verifies that a transaction held by
// IncludeInLowWatermark no longer holds back the checkpoint after it commits or
// rolls back.
func TestLOBTransactionReleasesCheckpointOnEnd(t *testing.T) {
	for _, end := range []sqlredo.Operation{sqlredo.OpRollback, sqlredo.OpCommit} {
		t.Run(end.String(), func(t *testing.T) {
			cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
			lm := newLogMiner(&publisherStub{}, cache)
			lm.cfg.LOBEnabled = true
			lm.lobColTypes = map[string]string{"TESTDB.T.DOC": "CLOB"}

			require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
				SCN: 100, Operation: sqlredo.OpStart, TransactionID: "txB",
			}))
			require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
				SCN:           110,
				Operation:     sqlredo.OpLobWrite,
				TransactionID: "txB",
				SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
				TableName:     sql.NullString{String: "T", Valid: true},
				SQLRedo:       sql.NullString{String: " buf_c := 'hello';\n  dbms_lob.write(loc_c, 5, 1, buf_c);", Valid: true},
			}))
			require.Equal(t, uint64(100), cache.LowWatermarkSCN(""), "txB must hold the checkpoint")

			require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
				SCN: 120, Operation: end, TransactionID: "txB",
			}))
			assert.Equal(t, uint64(math.MaxUint64), cache.LowWatermarkSCN(""))
		})
	}
}

// TestLOBOnlyTransactionWithoutStartIsPublished verifies that a transaction with
// only LOB events is published when its START is not mined. This occurs after a
// restart from a checkpoint between the START and the first LOB event.
func TestLOBOnlyTransactionWithoutStartIsPublished(t *testing.T) {
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	pub := &publisherStub{}
	lm := newLogMiner(pub, cache)
	lm.cfg.LOBEnabled = true
	lm.lobColTypes = map[string]string{"TESTDB.T.DOC": "CLOB"}

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           110,
		Operation:     sqlredo.OpSelectLobLocator,
		TransactionID: "txB",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: `declare lob_1 clob; begin select "DOC" into lob_1 from "TESTDB"."T" where "ID" = '42';`, Valid: true},
	}))
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           111,
		Operation:     sqlredo.OpLobWrite,
		TransactionID: "txB",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "T", Valid: true},
		SQLRedo:       sql.NullString{String: " buf_c := 'hello';\n  dbms_lob.write(loc_c, 5, 1, buf_c);", Valid: true},
	}))
	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 140, Operation: sqlredo.OpCommit, TransactionID: "txB",
	}))

	require.Len(t, pub.messages, 1, "the synthesized UPDATE must be published")
	data, ok := pub.messages[0].Data.(map[string]any)
	require.True(t, ok, "Data should be map[string]any")
	assert.Equal(t, "hello", data["DOC"])
	assert.Equal(t, replication.SCN(140), pub.messages[0].CheckpointSCN)
}

func newLogMiner(pub replication.ChangePublisher, cache TransactionCache) *LogMiner {
	return &LogMiner{
		publisher:        pub,
		txnCache:         cache,
		dmlParser:        sqlredo.NewParser(),
		log:              service.NewLoggerFromSlog(slog.Default()),
		cfg:              NewDefaultConfig(),
		lobStates:        make(map[sqlredo.TransactionID]*sqlredo.TxnLOBState),
		pendingLOBWrites: make(map[sqlredo.TransactionID][]*sqlredo.RedoEvent),
	}
}

type publisherStub struct{ messages []*replication.MessageEvent }

func (p *publisherStub) Publish(_ context.Context, msg *replication.MessageEvent) error {
	p.messages = append(p.messages, msg)
	return nil
}

func (*publisherStub) Close() {}

// TestIncludeInLowWatermark verifies IncludeInLowWatermark on each transaction
// cache implementation. A discarded transaction is one that exceeded the max
// event buffer of 1.
func TestIncludeInLowWatermark(t *testing.T) {
	ctx := t.Context()
	dml := &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"}

	caches := map[string]func(maxEvents int) TransactionCache{
		"in memory": func(maxEvents int) TransactionCache {
			return NewInMemoryCache(maxEvents, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
		},
		"connect cache resource": func(maxEvents int) TransactionCache {
			res := service.MockResources(service.MockResourcesOptAddCache("txn_cache"))
			cfg := TransactionCacheConfig{CacheName: "txn_cache", CacheKey: "oracledb_cdc", MaxEvents: maxEvents}
			return NewConnectCacheResource(res, cfg, res.Metrics(), service.NewLoggerFromSlog(slog.Default()))
		},
	}

	tests := []struct {
		name         string
		maxEvents    int
		excludeTxnID sqlredo.TransactionID
		setup        func(t *testing.T, c TransactionCache)
		want         uint64
	}{
		{
			name: "marked transaction without events is counted at its start SCN",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
			},
			want: 100,
		},
		{
			name: "unmarked transaction without events is not counted",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
			},
			want: math.MaxUint64,
		},
		{
			name: "marking is idempotent",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
			},
			want: 100,
		},
		{
			name: "a later event does not change the start SCN",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
				require.NoError(t, c.AddEvent(ctx, "txA", 150, dml))
			},
			want: 100,
		},
		{
			name: "marking a missing transaction does nothing",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.IncludeInLowWatermark(ctx, "missing"))
			},
			want: math.MaxUint64,
		},
		{
			name:      "marking a discarded transaction does nothing",
			maxEvents: 1,
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
				require.NoError(t, c.AddEvent(ctx, "txA", 101, dml))
				require.NoError(t, c.AddEvent(ctx, "txA", 102, dml))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
			},
			want: math.MaxUint64,
		},
		{
			name:         "excluded transaction is not counted",
			excludeTxnID: "txA",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
				require.NoError(t, c.StartTransaction(ctx, "txB", 200))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txB"))
			},
			want: 200,
		},
		{
			name: "committed transaction is no longer counted",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
				require.NoError(t, c.CommitTransaction(ctx, "txA"))
			},
			want: math.MaxUint64,
		},
		{
			name: "rolled back transaction is no longer counted",
			setup: func(t *testing.T, c TransactionCache) {
				require.NoError(t, c.StartTransaction(ctx, "txA", 100))
				require.NoError(t, c.IncludeInLowWatermark(ctx, "txA"))
				require.NoError(t, c.RollbackTransaction(ctx, "txA"))
			},
			want: math.MaxUint64,
		},
	}
	for cacheName, newCache := range caches {
		for _, test := range tests {
			t.Run(cacheName+"/"+test.name, func(t *testing.T) {
				c := newCache(test.maxEvents)
				test.setup(t, c)
				assert.Equal(t, test.want, c.LowWatermarkSCN(test.excludeTxnID))
			})
		}
	}
}
