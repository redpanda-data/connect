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
	"errors"
	"fmt"
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
	// persistMu serializes the whole resolve+compute+write+commit sequence
	// for one segment. Acks for a segment's batches arrive on concurrent
	// pipeline goroutines: without this, two acks can both trip the persist
	// check and race their store writes, landing an older position (or a
	// Complete=false row) over a newer one. Always acquired before t.mu.
	persistMu sync.Mutex
	tracker   *checkpoint.Uncapped[segmentCheckpoint]
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
	t.mu.Unlock()
	if !ok {
		return nil
	}

	// One ack at a time per segment, held across the store write: each
	// persist is computed after the previous write finished, so the durable
	// row can only move forward, and the interval check cannot double-fire.
	st.persistMu.Lock()
	defer st.persistMu.Unlock()

	t.mu.Lock()
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
	ackedAtCompute := st.ackedBatches
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
		// Bookkeeping is deliberately untouched so the failed position is
		// never treated as durable. Interval persists are retried by the
		// segment's next ack; a failed COMPLETION write has no later ack to
		// retry it (the seal is the segment's last settle event), so it is
		// re-driven by FlushCompleted once the snapshot's ack gate drains.
		return persistErr
	}

	t.mu.Lock()
	st.persistedBatches = ackedAtCompute
	if toPersist.complete {
		st.persistedComplete = true
	}
	t.mu.Unlock()
	return nil
}

// FlushCompleted re-drives the Complete=true write for every segment whose
// frontier has fully resolved to its seal marker but whose completion was
// never durably persisted - a throttled completion PutItem otherwise stays
// lost forever, and the next run re-scans the segment's tail from a stale
// position. Called after the snapshot ack gate has drained, so no acks are
// in flight; per-segment persistMu is still taken for consistency.
func (t *snapshotAckTracker) FlushCompleted(ctx context.Context) error {
	t.mu.Lock()
	var pending []int
	for seg, st := range t.segments {
		if st.hasFrontier && st.frontier.complete && !st.persistedComplete {
			pending = append(pending, seg)
		}
	}
	t.mu.Unlock()

	var errs []error
	for _, seg := range pending {
		t.mu.Lock()
		st := t.segments[seg]
		records := st.ackedRecords
		t.mu.Unlock()

		st.persistMu.Lock()
		err := t.store.UpdateSnapshotProgress(ctx, seg, nil, records)
		if err == nil {
			t.mu.Lock()
			st.persistedComplete = true
			t.mu.Unlock()
		}
		st.persistMu.Unlock()
		if err != nil {
			errs = append(errs, fmt.Errorf("re-driving completion for segment %d: %w", seg, err))
		}
	}
	return errors.Join(errs...)
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
