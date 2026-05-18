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

		cache.StartTransaction("txA", txAStart)
		cache.AddEvent("txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"})

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
		cache.StartTransaction("txA", txAStart)
		cache.AddEvent("txA", txAStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"})
		cache.StartTransaction("txB", txBStart)
		cache.AddEvent("txB", txBStart, &sqlredo.DMLEvent{Operation: sqlredo.OpInsert, Table: "T"})

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
}

func newLogMiner(pub replication.ChangePublisher, cache TransactionCache) *LogMiner {
	return &LogMiner{
		publisher: pub,
		txnCache:  cache,
		dmlParser: sqlredo.NewParser(),
		log:       service.NewLoggerFromSlog(slog.Default()),
		cfg:       NewDefaultConfig(),
		lobStates: make(map[sqlredo.TransactionID]*sqlredo.TxnLOBState),
	}
}

type publisherStub struct{ messages []*replication.MessageEvent }

func (p *publisherStub) Publish(_ context.Context, msg *replication.MessageEvent) error {
	p.messages = append(p.messages, msg)
	return nil
}

func (*publisherStub) Close() {}
