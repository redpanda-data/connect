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
	"log/slog"
	"sync"
	"testing"

	"github.com/Jeffail/checkpoint"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

func TestPublishBatch(t *testing.T) {
	t.Run("snapshot batches never persist via ackFn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})
		msg1 := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage("100")})

		require.NoError(t, msg.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called after acking only the first snapshot batch")

		require.NoError(t, msg1.ackFn(ctx, nil))
		require.Empty(t, cachedSCNs(), "cacheSCN must not be called after acking all snapshot batches")
	})

	t.Run("streaming batch still persists via ackFn", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		batch := service.MessageBatch{newStreamingMessage("200")}
		msg := publishAndReceive(t, ctx, publisher, batch)
		require.NoError(t, msg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once for a streaming batch")
		require.Equal(t, replication.SCN(200), scns[0])
	})

	t.Run("mixed snapshot and streaming batch persists", func(t *testing.T) {
		ctx := t.Context()
		publisher, cachedSCNs := newTestBatchPublisher(t)

		batch := service.MessageBatch{
			newSnapshotMessage("100"),
			newStreamingMessage("300"),
		}
		msg := publishAndReceive(t, ctx, publisher, batch)
		require.NoError(t, msg.ackFn(ctx, nil))

		scns := cachedSCNs()
		require.Len(t, scns, 1, "expected cacheSCN to be called exactly once for a batch whose last message is streaming")
		require.Equal(t, replication.SCN(300), scns[0], "expected the streaming SCN from the last message, not the leftover snapshot SCN")
	})
}

func newTestBatchPublisher(t *testing.T) (*batchPublisher, func() []replication.SCN) {
	t.Helper()

	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.SCN](100)

	publisher := newBatchPublisher(nil, cp, logger)
	t.Cleanup(publisher.Close)

	var (
		mu         sync.Mutex
		cachedSCNs []replication.SCN
	)
	publisher.cacheSCN = func(_ context.Context, scn replication.SCN) error {
		mu.Lock()
		defer mu.Unlock()
		cachedSCNs = append(cachedSCNs, scn)
		return nil
	}

	cachedSCNsFn := func() []replication.SCN {
		mu.Lock()
		defer mu.Unlock()
		return append([]replication.SCN(nil), cachedSCNs...)
	}

	return publisher, cachedSCNsFn
}

func newSnapshotMessage(scn string) *service.Message {
	msg := service.NewMessage([]byte("{}"))
	msg.MetaSet("operation", replication.MessageOperationRead.String())
	msg.MetaSet("scn", scn)
	return msg
}

func newStreamingMessage(checkpointSCN string) *service.Message {
	msg := service.NewMessage([]byte("{}"))
	msg.MetaSet("operation", replication.MessageOperationInsert.String())
	msg.MetaSet("checkpoint_scn", checkpointSCN)
	return msg
}

func publishAndReceive(t *testing.T, ctx context.Context, publisher *batchPublisher, batch service.MessageBatch) asyncMessage {
	t.Helper()
	go func() {
		_ = publisher.publishBatch(ctx, batch)
	}()
	return <-publisher.msgs()
}
