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
	"sync"

	"github.com/Jeffail/checkpoint"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// defaultSnapshotCheckpointBatchInterval is how many acknowledged batches a
// segment accumulates between checkpoint-store writes (bounding write volume
// the same way the pre-ack-gating scanner checkpointed every 10 batches).
const defaultSnapshotCheckpointBatchInterval = 10

// snapshotProgressStore is the persistence surface used by the snapshot ack
// tracker; satisfied by *Checkpointer.
type snapshotProgressStore interface {
	UpdateSnapshotProgress(ctx context.Context, segment int, lastKey map[string]dynamodbtypes.AttributeValue, recordsRead int64) error
}

// segmentCheckpoint is the ordered-tracker payload for a snapshot segment:
// either a scan resume position (lastKey) or the segment-complete marker.
type segmentCheckpoint struct {
	lastKey  map[string]dynamodbtypes.AttributeValue
	complete bool
}

type segmentAckState struct {
	tracker *checkpoint.Uncapped[segmentCheckpoint]
	// frontier is the highest contiguous acked checkpoint.
	frontier    segmentCheckpoint
	hasFrontier bool
	// ackedRecords/ackedBatches accumulate acknowledged progress; persistence
	// is throttled to every interval batches (plus completion).
	ackedRecords      int64
	ackedBatches      int
	persistedBatches  int
	persistedComplete bool
}

// snapshotAckTracker gates snapshot progress persistence on downstream acks.
// Batches are tracked per segment in scan order; UpdateSnapshotProgress is
// only ever called with the highest *contiguous* acknowledged position, so a
// crash never skips unacked snapshot items on resume, and a segment is only
// marked complete once every one of its batches has been acknowledged.
type snapshotAckTracker struct {
	store    snapshotProgressStore
	interval int
	log      *service.Logger

	mu       sync.Mutex
	segments map[int]*segmentAckState
}

func newSnapshotAckTracker(store snapshotProgressStore, interval int, log *service.Logger) *snapshotAckTracker {
	if interval <= 0 {
		interval = defaultSnapshotCheckpointBatchInterval
	}
	return &snapshotAckTracker{
		store:    store,
		interval: interval,
		log:      log,
		segments: make(map[int]*segmentAckState),
	}
}

func (t *snapshotAckTracker) segmentLocked(segment int) *segmentAckState {
	st, ok := t.segments[segment]
	if !ok {
		st = &segmentAckState{tracker: checkpoint.NewUncapped[segmentCheckpoint]()}
		t.segments[segment] = st
	}
	return st
}

// TrackBatch registers a snapshot batch with its segment's ordered tracker and
// returns the resolve function for its ack. Must be called in scan order per
// segment (the segment's single scan goroutine).
func (t *snapshotAckTracker) TrackBatch(segment int, lastKey map[string]dynamodbtypes.AttributeValue, n int) func() *segmentCheckpoint {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.segmentLocked(segment).tracker.Track(segmentCheckpoint{lastKey: lastKey}, int64(n))
}

// Ack marks a tracked batch as acknowledged and persists the segment's
// contiguous frontier once enough batches have been acked since the last
// persist (or immediately when the frontier reaches the completion marker).
func (t *snapshotAckTracker) Ack(ctx context.Context, segment, n int, resolve func() *segmentCheckpoint) error {
	t.mu.Lock()
	st, ok := t.segments[segment]
	if !ok {
		t.mu.Unlock()
		return nil
	}
	if fr := resolve(); fr != nil {
		st.frontier = *fr
		st.hasFrontier = true
	}
	st.ackedRecords += int64(n)
	st.ackedBatches++

	var (
		toPersist *segmentCheckpoint
		records   int64
	)
	if st.hasFrontier {
		completeDue := st.frontier.complete && !st.persistedComplete
		intervalDue := st.frontier.lastKey != nil && st.ackedBatches-st.persistedBatches >= t.interval
		if completeDue || intervalDue {
			cp := st.frontier
			toPersist = &cp
			records = st.ackedRecords
		}
	}
	t.mu.Unlock()

	if toPersist == nil {
		return nil
	}
	var persistErr error
	if toPersist.complete {
		persistErr = t.store.UpdateSnapshotProgress(ctx, segment, nil, records)
	} else {
		persistErr = t.store.UpdateSnapshotProgress(ctx, segment, toPersist.lastKey, records)
	}
	if persistErr != nil {
		// Bookkeeping is deliberately untouched: the next ack (or seal) for
		// this segment retries the write instead of silently treating the
		// failed position as durable.
		return persistErr
	}

	t.mu.Lock()
	st.persistedBatches = st.ackedBatches
	if toPersist.complete {
		st.persistedComplete = true
	}
	t.mu.Unlock()
	return nil
}

// SealSegment registers the segment-complete marker. Segments can end on an
// empty scan page, so completion cannot ride on a final batch; the marker
// resolves immediately and Complete=true persists as soon as every batch
// before it has been acknowledged (possibly inside a later Ack call).
func (t *snapshotAckTracker) SealSegment(ctx context.Context, segment int) error {
	t.mu.Lock()
	resolve := t.segmentLocked(segment).tracker.Track(segmentCheckpoint{complete: true}, 0)
	t.mu.Unlock()
	return t.Ack(ctx, segment, 0, resolve)
}
