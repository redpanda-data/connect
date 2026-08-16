// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSnapshotAckFn(t *testing.T) {
	noPersist := func(context.Context, bson.Raw) error {
		return errors.New("persist must not be called for a nil token")
	}

	t.Run("a nack resolves too: auto_replay_nacks off is an opt-in drop", func(t *testing.T) {
		resolved := false
		ackFn := snapshotAckFn(func() *bson.Raw {
			resolved = true
			return nil
		}, noPersist)
		require.NoError(t, ackFn(t.Context(), errors.New("downstream failure")))
		require.True(t, resolved, "a nacked batch is deleted per the auto_replay_nacks contract; the stream must continue past it")
	})

	t.Run("ack resolves and accepts a nil resume token", func(t *testing.T) {
		resolved := false
		ackFn := snapshotAckFn(func() *bson.Raw {
			resolved = true
			return nil
		}, noPersist)
		require.NoError(t, ackFn(t.Context(), nil))
		require.True(t, resolved)
	})

	t.Run("a streaming token surfaced by out-of-order acks is persisted", func(t *testing.T) {
		// Snapshot and streaming share one ordered tracker: when a streaming
		// batch acks before an earlier snapshot batch, the snapshot slot's
		// resolve legitimately returns the streaming token as the contiguous
		// frontier. It must be persisted, not dropped.
		token := bson.Raw("streaming-token")
		var persisted bson.Raw
		ackFn := snapshotAckFn(func() *bson.Raw { return &token }, func(_ context.Context, tok bson.Raw) error {
			persisted = tok
			return nil
		})
		require.NoError(t, ackFn(t.Context(), nil))
		require.Equal(t, token, persisted, "the resolved streaming checkpoint must persist through the same path as a streaming ack")
	})
}
