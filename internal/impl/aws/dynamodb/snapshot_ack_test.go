// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type recordedProgress struct {
	segment     int
	lastKey     map[string]dynamodbtypes.AttributeValue
	recordsRead int64
}

type fakeProgressStore struct {
	mu      sync.Mutex
	updates []recordedProgress
}

func (f *fakeProgressStore) UpdateSnapshotProgress(_ context.Context, segment int, lastKey map[string]dynamodbtypes.AttributeValue, recordsRead int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, recordedProgress{segment: segment, lastKey: lastKey, recordsRead: recordsRead})
	return nil
}

func (f *fakeProgressStore) recorded() []recordedProgress {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedProgress(nil), f.updates...)
}

func scanKey(v string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"pk": &dynamodbtypes.AttributeValueMemberS{Value: v},
	}
}

func keyVal(k map[string]dynamodbtypes.AttributeValue) string {
	if s, ok := k["pk"].(*dynamodbtypes.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func newTestSnapshotAckTracker(interval int) (*snapshotAckTracker, *fakeProgressStore) {
	store := &fakeProgressStore{}
	return newSnapshotAckTracker(store, interval, service.NewLoggerFromSlog(slog.Default())), store
}

func TestSnapshotAckTracker(t *testing.T) {
	ctx := context.Background()

	t.Run("persists only acknowledged positions", func(t *testing.T) {
		tracker, store := newTestSnapshotAckTracker(1)

		r1 := tracker.TrackBatch(0, scanKey("k1"), 5)
		require.Empty(t, store.recorded(), "nothing may persist at read time")

		require.NoError(t, tracker.Ack(ctx, 0, 5, r1))
		got := store.recorded()
		require.Len(t, got, 1)
		require.Equal(t, "k1", keyVal(got[0].lastKey))
		require.Equal(t, int64(5), got[0].recordsRead)
	})

	t.Run("out-of-order acks never persist past an unacked batch", func(t *testing.T) {
		tracker, store := newTestSnapshotAckTracker(1)

		r1 := tracker.TrackBatch(0, scanKey("k1"), 5)
		r2 := tracker.TrackBatch(0, scanKey("k2"), 5)

		// Acking only the LATER batch must not persist k2: the items before
		// it are not durable. (The pre-fix code persisted at read time - the
		// data-loss window.)
		require.NoError(t, tracker.Ack(ctx, 0, 5, r2))
		require.Empty(t, store.recorded(), "the frontier must not advance past a still-unacked earlier batch")

		require.NoError(t, tracker.Ack(ctx, 0, 5, r1))
		got := store.recorded()
		require.Len(t, got, 1)
		require.Equal(t, "k2", keyVal(got[0].lastKey), "acking the gap resolves the full prefix")
	})

	t.Run("interval throttles persistence", func(t *testing.T) {
		tracker, store := newTestSnapshotAckTracker(3)

		var resolves []func() *segmentCheckpoint
		for i := range 3 {
			resolves = append(resolves, tracker.TrackBatch(0, scanKey(string(rune('a'+i))), 1))
		}
		require.NoError(t, tracker.Ack(ctx, 0, 1, resolves[0]))
		require.NoError(t, tracker.Ack(ctx, 0, 1, resolves[1]))
		require.Empty(t, store.recorded(), "persistence is throttled to every interval batches")
		require.NoError(t, tracker.Ack(ctx, 0, 1, resolves[2]))
		require.Len(t, store.recorded(), 1)
	})

	t.Run("seal persists completion only after all batches ack", func(t *testing.T) {
		tracker, store := newTestSnapshotAckTracker(1000) // interval never triggers

		r1 := tracker.TrackBatch(1, scanKey("k1"), 5)
		require.NoError(t, tracker.SealSegment(ctx, 1))
		require.Empty(t, store.recorded(), "completion must wait for the segment's in-flight batches")

		require.NoError(t, tracker.Ack(ctx, 1, 5, r1))
		got := store.recorded()
		require.Len(t, got, 1)
		require.Nil(t, got[0].lastKey, "a nil lastKey marks the segment complete")
		require.Equal(t, 1, got[0].segment)
		require.Equal(t, int64(5), got[0].recordsRead)
	})

	t.Run("seal on an empty segment persists completion immediately", func(t *testing.T) {
		tracker, store := newTestSnapshotAckTracker(1000)

		require.NoError(t, tracker.SealSegment(ctx, 2))
		got := store.recorded()
		require.Len(t, got, 1)
		require.Nil(t, got[0].lastKey)
	})

	t.Run("a never-acked batch pins the segment forever", func(t *testing.T) {
		tracker, store := newTestSnapshotAckTracker(1)

		tracker.TrackBatch(3, scanKey("k1"), 5) // nacked: resolve never called
		r2 := tracker.TrackBatch(3, scanKey("k2"), 5)
		require.NoError(t, tracker.Ack(ctx, 3, 5, r2))
		require.NoError(t, tracker.SealSegment(ctx, 3))

		require.Empty(t, store.recorded(), "neither progress nor completion may pass a nacked batch")
	})
}
