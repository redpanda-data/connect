// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"
)

func TestSnapshotAckGate(t *testing.T) {
	t.Run("blocks until the snapshot batch is acked", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage()})

		done := make(chan error, 1)
		go func() { done <- publisher.waitSnapshotAcks(ctx) }()

		select {
		case err := <-done:
			t.Fatalf("waitSnapshotAcks returned before the snapshot batch was acked: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		require.NoError(t, msg.ackFn(ctx, nil))
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("waitSnapshotAcks did not return after the snapshot batch was acked")
		}
	})

	t.Run("a nack also releases the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		msg := publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage()})
		require.NoError(t, msg.ackFn(ctx, errors.New("downstream failure")))

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("streaming batches do not hold the gate", func(t *testing.T) {
		ctx := t.Context()
		publisher, _ := newTestBatchPublisher(t)

		// Published but never acked: must not block the gate.
		publishAndReceive(t, ctx, publisher, service.MessageBatch{newStreamingMessage("00000030")})

		require.NoError(t, publisher.waitSnapshotAcks(ctx))
	})

	t.Run("context cancellation escapes the gate", func(t *testing.T) {
		publisher, _ := newTestBatchPublisher(t)

		ctx, cancel := context.WithCancel(t.Context())
		publishAndReceive(t, ctx, publisher, service.MessageBatch{newSnapshotMessage()})

		done := make(chan error, 1)
		go func() { done <- publisher.waitSnapshotAcks(ctx) }()
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("waitSnapshotAcks did not return after context cancellation")
		}
	})
}

func TestFlushCurrent(t *testing.T) {
	ctx := t.Context()
	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.LSN](100)

	batcher, err := (service.BatchPolicy{Count: 100}).NewBatcher(service.MockResources())
	require.NoError(t, err)

	publisher := newBatchPublisher(batcher, cp, logger)
	t.Cleanup(func() { publisher.shutSig.TriggerSoftStop() })
	publisher.cacheLSN = func(context.Context, replication.LSN) error { return nil }

	publishEvent := func() {
		t.Helper()
		require.NoError(t, publisher.Publish(ctx, replication.MessageEvent{
			Schema:    "dbo",
			Table:     "t",
			Operation: replication.MessageOperationRead.String(),
			Data:      map[string]any{"a": 1},
		}))
	}
	receive := func(failMsg string) {
		t.Helper()
		got := make(chan asyncMessage, 1)
		go func() { got <- <-publisher.msgs() }()
		require.NoError(t, publisher.flushCurrent(ctx))
		select {
		case m := <-got:
			require.Len(t, m.msg, 1)
		case <-time.After(5 * time.Second):
			t.Fatal(failMsg)
		}
	}

	// Count=100 keeps a single event buffered in the batcher until flushed.
	publishEvent()
	receive("flushCurrent did not publish the buffered partial batch")

	// The loop must still be alive after flushCurrent: a second
	// publish+flush must work identically.
	publishEvent()
	receive("publisher loop no longer functional after flushCurrent")
}

func newTestBatchPublisher(t *testing.T) (*batchPublisher, func() []replication.LSN) {
	t.Helper()

	logger := service.NewLoggerFromSlog(slog.Default())
	cp := checkpoint.NewCapped[replication.LSN](100)

	publisher := newBatchPublisher(nil, cp, logger)
	t.Cleanup(func() { publisher.shutSig.TriggerSoftStop() })

	var (
		mu         sync.Mutex
		cachedLSNs []replication.LSN
	)
	publisher.cacheLSN = func(_ context.Context, lsn replication.LSN) error {
		mu.Lock()
		defer mu.Unlock()
		cachedLSNs = append(cachedLSNs, lsn)
		return nil
	}

	cachedLSNsFn := func() []replication.LSN {
		mu.Lock()
		defer mu.Unlock()
		return append([]replication.LSN(nil), cachedLSNs...)
	}

	return publisher, cachedLSNsFn
}

func newSnapshotMessage() *service.Message {
	msg := service.NewMessage([]byte("{}"))
	msg.MetaSet("operation", replication.MessageOperationRead.String())
	return msg
}

func newStreamingMessage(lsn string) *service.Message {
	msg := service.NewMessage([]byte("{}"))
	msg.MetaSet("operation", replication.MessageOperationInsert.String())
	msg.MetaSet("lsn", lsn)
	return msg
}

func publishAndReceive(t *testing.T, ctx context.Context, publisher *batchPublisher, batch service.MessageBatch) asyncMessage {
	t.Helper()
	go func() {
		_ = publisher.publishBatch(ctx, batch)
	}()
	return <-publisher.msgs()
}
