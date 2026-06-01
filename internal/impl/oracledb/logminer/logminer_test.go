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

// TestLOBWriteBeforeInsert verifies that LOB_WRITE events buffered before their matching
// INSERT (BASICFILE DISABLE STORAGE IN ROW ordering) are correctly assembled when the
// INSERT is subsequently processed and drained.
func TestLOBWriteBeforeInsert(t *testing.T) {
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	pub := &publisherStub{}
	lm := newLogMiner(pub, cache)
	lm.cfg.LOBEnabled = true
	lm.lobColTypes = map[string]string{
		"TESTDB.ORDERS.NOTES": "CLOB",
	}

	ctx := t.Context()

	// OpStart
	require.NoError(t, lm.processRedoEvent(ctx, &sqlredo.RedoEvent{
		SCN: 100, Operation: sqlredo.OpStart, TransactionID: "tx1",
	}))

	// Two LOB_WRITE fragments arrive BEFORE the INSERT — BASICFILE out-of-line ordering.
	for _, fragSQL := range []string{
		"buf_c := 'Hello ';\ndbms_lob.write(loc_c, 6, 1, buf_c);",
		"buf_c := 'World';\ndbms_lob.write(loc_c, 5, 7, buf_c);",
	} {
		require.NoError(t, lm.processRedoEvent(ctx, &sqlredo.RedoEvent{
			SCN:           101,
			Operation:     sqlredo.OpLobWrite,
			TransactionID: "tx1",
			SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
			TableName:     sql.NullString{String: "ORDERS", Valid: true},
			SQLRedo:       sql.NullString{String: fragSQL, Valid: true},
		}))
	}

	// INSERT arrives after the LOB_WRITEs; NOTES column is absent from SQL_REDO
	// because BASICFILE out-of-line storage omits the column from the INSERT row.
	require.NoError(t, lm.processRedoEvent(ctx, &sqlredo.RedoEvent{
		SCN:           102,
		Operation:     sqlredo.OpInsert,
		TransactionID: "tx1",
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "ORDERS", Valid: true},
		SQLRedo:       sql.NullString{String: `insert into "TESTDB"."ORDERS"("ID","STATUS") values (42,'open')`, Valid: true},
	}))

	// Commit
	require.NoError(t, lm.processRedoEvent(ctx, &sqlredo.RedoEvent{
		SCN: 103, Operation: sqlredo.OpCommit, TransactionID: "tx1",
	}))

	require.Len(t, pub.messages, 1)
	assert.Equal(t, "Hello World", pub.messages[0].Data.(map[string]any)["NOTES"])
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
