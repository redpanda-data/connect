// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func newNackTestExecutor(t *testing.T) *salesforceCDCInputExecutor {
	t.Helper()

	mgr := service.MockResources(service.MockResourcesOptAddCache("cpcache"))
	e := &salesforceCDCInputExecutor{
		salesforceCDCInput: &salesforceCDCInput{
			conf: CDCInputConfig{
				Checkpoint: CheckpointConfig{Cache: "cpcache", CacheKey: "state", Limit: 16},
			},
			mgr:    mgr,
			logger: service.NewLoggerFromSlog(slog.Default()),
		},
		msgChan: make(chan asyncMessage, 8),
		stopSig: shutdown.NewSignaller(),
		state:   executorState{Topics: TopicReplays{}},
	}
	return e
}

// TestFlushTopicNackPinsCheckpoint locks in the streaming ack semantics: a
// nack must never resolve its checkpoint slot, so no replay ID can be
// persisted past a batch that was rejected downstream (auto_replay_nacks is
// user-toggleable, so a nack can be terminal).
func TestFlushTopicNackPinsCheckpoint(t *testing.T) {
	ctx := t.Context()
	e := newNackTestExecutor(t)
	cp := checkpoint.NewCapped[[]byte](16)

	batcher, err := (service.BatchPolicy{Count: 1}).NewBatcher(service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = batcher.Close(ctx) })

	flushOne := func(replayID []byte) asyncMessage {
		t.Helper()
		batcher.Add(service.NewMessage([]byte("{}")))
		require.NoError(t, e.flushTopic(ctx, "/data/AccountChangeEvent", batcher, cp, replayID))
		return <-e.msgChan
	}

	b1 := flushOne([]byte{0x01})
	b2 := flushOne([]byte{0x02})

	// Nack the first batch: terminal with auto_replay_nacks disabled. The
	// second batch's ack must not persist anything past the rejected one.
	nackErr := errors.New("downstream failure")
	require.ErrorIs(t, b1.ackFn(ctx, nackErr), nackErr)
	require.NoError(t, b2.ackFn(ctx, nil))

	e.stateMu.Lock()
	_, exists := e.state.Topics["/data/AccountChangeEvent"]
	e.stateMu.Unlock()
	require.False(t, exists, "a replay ID must never be persisted past a nacked batch")
}

// TestEmitSnapshotNackPinsCheckpoint locks in the snapshot ack semantics: a
// nacked snapshot batch must not advance the persisted snapshot cursor or the
// SnapshotComplete flag.
func TestEmitSnapshotNackPinsCheckpoint(t *testing.T) {
	ctx := t.Context()
	e := newNackTestExecutor(t)
	cp := checkpoint.NewCapped[*executorState](16)

	snap1 := &executorState{SnapshotComplete: false}
	snap2 := &executorState{SnapshotComplete: true}

	require.NoError(t, e.emitSnapshot(ctx, cp, service.MessageBatch{service.NewMessage([]byte("{}"))}, snap1))
	b1 := <-e.msgChan
	require.NoError(t, e.emitSnapshot(ctx, cp, service.MessageBatch{service.NewMessage([]byte("{}"))}, snap2))
	b2 := <-e.msgChan

	nackErr := errors.New("downstream failure")
	require.ErrorIs(t, b1.ackFn(ctx, nackErr), nackErr)
	require.NoError(t, b2.ackFn(ctx, nil))

	e.stateMu.Lock()
	complete := e.state.SnapshotComplete
	e.stateMu.Unlock()
	require.False(t, complete, "SnapshotComplete must never be persisted past a nacked snapshot batch")
}
