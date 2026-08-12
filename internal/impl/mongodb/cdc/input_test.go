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
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSpecParsesAWSBlock(t *testing.T) {
	sb := service.NewStreamBuilder()
	err := sb.AddInputYAML(`
mongodb_cdc:
  url: "mongodb://localhost:27017"
  database: foo
  collections: [bar]
  checkpoint_cache: foocache
  aws:
    enabled: true
    region: us-east-1
    roles:
      - role: arn:aws:iam::123456789012:role/foo
`)
	require.NoError(t, err)
}

func TestStoreSnapshotCheckpointWaitsForSnapshotAcks(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	resolve, err := cp.Track(context.Background(), nil, 5)
	require.NoError(t, err)
	require.Equal(t, int64(5), cp.Pending())

	m := &mongoCDC{}
	token := bson.Raw{5, 0, 0, 0, 0}
	stored := make(chan bson.Raw, 1)
	proceed := make(chan bool, 1)
	go func() {
		proceed <- m.storeSnapshotCheckpoint(context.Background(), cp, token, func(_ context.Context, rt bson.Raw) error {
			stored <- rt
			return nil
		})
	}()

	// While a snapshot batch is unresolved nothing may be persisted: a restart
	// loading that checkpoint would skip the undelivered part of the snapshot.
	select {
	case rt := <-stored:
		t.Fatalf("checkpoint stored while a snapshot batch was in flight: %v", rt)
	case <-proceed:
		t.Fatal("returned while a snapshot batch was in flight")
	case <-time.After(3 * snapshotAckPollInterval):
	}
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken)
	m.resumeTokenMu.Unlock()

	resolve()

	select {
	case rt := <-stored:
		require.Equal(t, token, rt)
	case <-time.After(time.Minute):
		t.Fatal("checkpoint was not stored after the snapshot batch resolved")
	}
	require.True(t, <-proceed)
	m.resumeTokenMu.Lock()
	require.Equal(t, token, m.resumeToken)
	m.resumeTokenMu.Unlock()
}

func TestStoreSnapshotCheckpointStopsOnShutdown(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	_, err := cp.Track(context.Background(), nil, 1)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	m := &mongoCDC{}
	proceed := make(chan bool, 1)
	go func() {
		proceed <- m.storeSnapshotCheckpoint(ctx, cp, bson.Raw{5, 0, 0, 0, 0}, func(context.Context, bson.Raw) error {
			t.Error("checkpoint stored despite an unresolved snapshot batch")
			return nil
		})
	}()
	cancel()

	select {
	case ok := <-proceed:
		require.False(t, ok)
	case <-time.After(time.Minute):
		t.Fatal("wait loop did not exit on context cancellation")
	}
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken)
	m.resumeTokenMu.Unlock()
}

func TestStoreSnapshotCheckpointSkippedOnCancelledContext(t *testing.T) {
	// Nothing is pending, but the context is already cancelled: the shutdown
	// paths resolve the slots of snapshot batches they drop without delivering
	// them, so a drained checkpointer proves nothing once the connection is
	// going away. Storing here would let a restart skip an incomplete snapshot.
	cp := checkpoint.NewCapped[bson.Raw](10)
	require.Zero(t, cp.Pending())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m := &mongoCDC{}
	require.False(t, m.storeSnapshotCheckpoint(ctx, cp, bson.Raw{5, 0, 0, 0, 0}, func(context.Context, bson.Raw) error {
		t.Error("checkpoint stored after the context was cancelled")
		return nil
	}))
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken)
	m.resumeTokenMu.Unlock()
}

func TestStoreSnapshotCheckpointSkippedWithoutToken(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	m := &mongoCDC{}
	require.True(t, m.storeSnapshotCheckpoint(context.Background(), cp, nil, func(context.Context, bson.Raw) error {
		t.Error("checkpoint stored without a resume token to store")
		return nil
	}))
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken)
	m.resumeTokenMu.Unlock()
}
