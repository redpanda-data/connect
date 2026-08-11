// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSnapshotAckFn(t *testing.T) {
	t.Run("a nack resolves too: auto_replay_nacks off is an opt-in drop", func(t *testing.T) {
		resolved := false
		ackFn := snapshotAckFn(func() *bson.Raw {
			resolved = true
			return nil
		})
		require.NoError(t, ackFn(t.Context(), errors.New("downstream failure")))
		require.True(t, resolved, "a nacked batch is deleted per the auto_replay_nacks contract; the stream must continue past it")
	})

	t.Run("ack resolves and accepts a nil resume token", func(t *testing.T) {
		resolved := false
		ackFn := snapshotAckFn(func() *bson.Raw {
			resolved = true
			return nil
		})
		require.NoError(t, ackFn(t.Context(), nil))
		require.True(t, resolved)
	})

	t.Run("ack rejects an unexpected non-nil resume token", func(t *testing.T) {
		token := bson.Raw("unexpected")
		ackFn := snapshotAckFn(func() *bson.Raw { return &token })
		require.Error(t, ackFn(t.Context(), nil))
	})
}
