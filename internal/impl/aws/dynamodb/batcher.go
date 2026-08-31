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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Throttle-pin visibility: a healthy backpressured input oscillates around
// the throttle threshold as downstream acks drain the tracker. Staying
// continuously at or above it means nothing is draining and every shard
// reader is parked - a wedge that is otherwise completely silent.
const (
	throttlePinWarnAfter    = 5 * time.Minute
	throttlePinWarnInterval = 5 * time.Minute
)

// RecordBatcher tracks in-flight message batches and persists shard
// checkpoints in stream order.
//
// Batches are tracked per shard in dispatch order via an ordered checkpoint
// tracker, so the persisted sequence number only ever advances to the highest
// *contiguous* acknowledged batch. Acks completing out of order (multiple
// batches in flight against a parallel output) can therefore never push the
// checkpoint past a batch that has not been acknowledged yet — a crash never
// skips unacked data on restart.
//
// A nacked batch is removed from tracking without being resolved, which pins
// the shard's checkpoint frontier at the position before that batch. Later
// acks keep accumulating but are not persisted past the gap; a restart
// resumes from the pinned checkpoint and redelivers the nacked records.
//
// To bound checkpoint-table writes, persistence happens only after at least
// checkpointLimit messages have been acknowledged on a shard since the last
// persisted checkpoint.
type RecordBatcher struct {
	maxTrackedShards   int
	maxTrackedMessages int
	log                *service.Logger

	mu sync.Mutex

	// shards holds the per-shard ordered ack trackers. Entries are never
	// removed; DynamoDB stream shards rotate within 24h so the map stays
	// small, and maxTrackedShards guards against pathological growth.
	shards map[string]*shardAckTracker
	// trackedMessages counts messages across all in-flight batches, used for
	// backpressure via ShouldThrottle.
	trackedMessages int
	// reserved counts budget handed out via TryReserve but not yet converted
	// into tracked messages (reads in flight). trackedMessages + reserved is
	// bounded by maxTrackedMessages, so concurrent readers can never
	// collectively overshoot the budget on their first read wave.
	reserved int
	// perShardCap bounds one shard's in-flight (tracked + reserved) messages
	// so a shard whose batches never settle downstream parks alone at its cap
	// instead of pinning the global budget and every other reader. A shard
	// with nothing in flight always admits one batch regardless, so progress
	// is guaranteed for any cap/batch-size combination.
	perShardCap int
	// throttledSince is when reservations last started failing on the global
	// budget without a successful reserve in between; zero while not
	// throttled. lastPinWarn rate-limits the pinned-throttle warning.
	throttledSince time.Time
	lastPinWarn    time.Time
}

// trackedBatch is the settlement handle for one dispatched batch, returned by
// AddMessages and captured by that batch's ack function. Settlement goes
// through this handle - never through a lookup keyed by the batch's message
// pointers - because wrappers between the input and the pipeline are free to
// rewrite the batch slice: benthos's AutoRetryNacksBatched replaces every
// element with a new *Message before the pipeline sees it, so a pointer-keyed
// lookup silently misses, the tracker never drains, and every shard reader
// parks in backpressure forever (INC-2974).
type trackedBatch struct {
	shardID string
	size    int
	// settled flips exactly once, whichever of ack/remove wins; repeat
	// settles are no-ops. Guarded by the batcher's mu.
	settled bool
	// resolve marks the batch as acknowledged in the shard's ordered tracker
	// and returns the new highest contiguous sequence, or nil if the frontier
	// did not move (an earlier batch is still outstanding).
	resolve func() *string
}

type shardAckTracker struct {
	// persistMu single-flights checkpoint writes for one shard (see
	// maybePersist): the holder computes each persist after its previous
	// write finished, so the durable row only ever moves forward, and every
	// other ack TryLocks past it - nothing queues behind a slow write, and
	// b.mu is never held across the write itself, so a hung checkpoint
	// PutItem cannot park ShouldThrottle/AddMessages and freeze the input.
	persistMu sync.Mutex
	tracker   *checkpoint.Uncapped[string]
	// pending counts acked messages since the last persisted checkpoint.
	pending int
	// frontier is the highest contiguous acked sequence ("" until the first
	// batch resolves in order).
	frontier string
	// persisted is the last sequence written to the checkpoint store.
	persisted string
	// seqTimes maps a tracked batch's checkpoint sequence to that record's
	// ApproximateCreationDateTime (RFC3339Nano). Used to persist the timestamp
	// alongside the frontier in global-table mode; entries are pruned once the
	// frontier advances past them. Only the batch frontier can advance, which
	// may differ from the batch currently being acked, so the lookup must be by
	// sequence rather than off the acked batch.
	seqTimes map[string]string
	// frontierTime is the ApproximateCreationDateTime of the current frontier.
	frontierTime string
	// inflight counts this shard's tracked (dispatched, unsettled) messages;
	// reserved counts budget handed to this shard's reader ahead of a read.
	// Together they are bounded by the batcher's perShardCap.
	inflight int
	reserved int
}

// NewRecordBatcher creates a new [RecordBatcher] for DynamoDB CDC.
func NewRecordBatcher(maxTrackedShards, checkpointLimit int, log *service.Logger) *RecordBatcher {
	// Set max tracked messages to 10x the checkpoint limit to allow for some buffering.
	// This prevents unbounded growth while allowing parallel processing.
	maxTrackedMessages := max(checkpointLimit*10,
		// Minimum reasonable size
		1000)

	return &RecordBatcher{
		maxTrackedShards:   maxTrackedShards,
		maxTrackedMessages: maxTrackedMessages,
		// One never-settling shard may pin at most a quarter of the budget;
		// the rest stays available to healthy shards (see perShardCap doc).
		perShardCap: maxTrackedMessages / 4,
		log:         log,
		shards:      make(map[string]*shardAckTracker),
	}
}

// TryReserve claims budget for a read of up to n messages on shardID. It
// refuses when the global tracked+reserved budget or the shard's in-flight
// cap has no room (the caller waits and retries), except that a shard with
// nothing in flight always admits one batch so progress is never impossible.
// Budget is consumed by AddMessages and any surplus must be returned via
// Release.
func (b *RecordBatcher) TryReserve(shardID string, n int) bool {
	if b == nil {
		// No batcher, no backpressure (mirrors ShouldThrottle's nil case).
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.trackedMessages+b.reserved+n > b.maxTrackedMessages {
		// Surface a continuously pinned budget: every shard reader parks on
		// this check, so if in-flight messages never settle the input is
		// stalled with no other signal.
		now := time.Now()
		if b.throttledSince.IsZero() {
			b.throttledSince = now
		} else if since := now.Sub(b.throttledSince); since >= throttlePinWarnAfter && now.Sub(b.lastPinWarn) >= throttlePinWarnInterval {
			b.lastPinWarn = now
			b.log.Warnf("Shard readers throttled for %v: %d/%d in-flight messages are still awaiting downstream acknowledgement (top shards: %s); no records are being read while this persists",
				since.Round(time.Second), b.trackedMessages, b.maxTrackedMessages, b.topInflightShardsLocked(3))
		}
		return false
	}

	st, ok := b.shards[shardID]
	if ok && st.inflight+st.reserved > 0 && st.inflight+st.reserved+n > b.perShardCap {
		// The shard is pinned by its own unsettled messages; park it alone
		// without touching the global throttle clock.
		return false
	}

	b.throttledSince = time.Time{}
	if !ok {
		st = &shardAckTracker{tracker: checkpoint.NewUncapped[string](), seqTimes: map[string]string{}}
		b.shards[shardID] = st
	}
	st.reserved += n
	b.reserved += n
	return true
}

// Release returns unused reservation for a shard (the read failed, returned
// fewer records than reserved, or every record was filtered out).
func (b *RecordBatcher) Release(shardID string, n int) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, ok := b.shards[shardID]; ok {
		released := min(n, st.reserved)
		st.reserved -= released
		b.reserved -= released
	}
}

// topInflightShardsLocked renders the k shards holding the most unsettled
// messages, for the pinned-throttle diagnostic. Callers must hold b.mu.
func (b *RecordBatcher) topInflightShardsLocked(k int) string {
	type entry struct {
		id string
		n  int
	}
	var entries []entry
	for id, st := range b.shards {
		if st.inflight > 0 {
			entries = append(entries, entry{id, st.inflight})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].n > entries[j].n })
	if len(entries) > k {
		entries = entries[:k]
	}
	parts := make([]string, len(entries))
	for i, e := range entries {
		parts[i] = fmt.Sprintf("%s=%d", e.id, e.n)
	}
	return strings.Join(parts, ", ")
}

// AddMessages tracks a batch of messages against a shard's ordered checkpoint
// tracker and returns the batch's settlement handle (nil for an empty batch),
// which the ack function must capture (see trackedBatch for why the batch
// itself cannot be the key). The batch must be in stream order (GetRecords
// returns records ordered by sequence number), so the last message's sequence
// number is the batch's checkpoint payload. Must be called from the shard's
// single reader goroutine so batches are tracked in dispatch order.
func (b *RecordBatcher) AddMessages(batch service.MessageBatch, shardID string) *trackedBatch {
	if len(batch) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if we're approaching memory limits
	if b.trackedMessages+len(batch) > b.maxTrackedMessages {
		b.log.Warnf("Message tracker near capacity: %d/%d tracked messages (adding %d from shard %s)",
			b.trackedMessages, b.maxTrackedMessages, len(batch), shardID)
		// Still add messages but warn - this indicates downstream is slow
	}

	st, ok := b.shards[shardID]
	if !ok {
		st = &shardAckTracker{tracker: checkpoint.NewUncapped[string](), seqTimes: map[string]string{}}
		b.shards[shardID] = st
	}

	// Convert the shard's outstanding reservation (if any) into tracked
	// messages; readers reserve at least the batch size before reading.
	consumed := min(st.reserved, len(batch))
	st.reserved -= consumed
	b.reserved -= consumed

	last := batch[len(batch)-1]
	seq, _ := last.MetaGet("dynamodb_sequence_number")
	approxCreationTime, _ := last.MetaGet("dynamodb_approximate_creation_time")
	st.seqTimes[seq] = approxCreationTime
	tb := &trackedBatch{
		shardID: shardID,
		size:    len(batch),
		resolve: st.tracker.Track(seq, int64(len(batch))),
	}
	b.trackedMessages += len(batch)
	st.inflight += len(batch)

	return tb
}

// settleLocked marks a batch settled and removes its message count, returning
// false if it had already settled. Callers must hold b.mu.
func (b *RecordBatcher) settleLocked(tb *trackedBatch) bool {
	if tb.settled {
		return false
	}
	tb.settled = true
	b.trackedMessages -= tb.size
	if st, ok := b.shards[tb.shardID]; ok {
		st.inflight -= tb.size
	}
	return true
}

// RemoveBatch drops a tracked batch without resolving it (used when messages
// are nacked, acked after close, or never dispatched). The shard's checkpoint
// frontier stays pinned before this batch, so later acks cannot persist past
// it and a restart redelivers the dropped records.
func (b *RecordBatcher) RemoveBatch(tb *trackedBatch) {
	if b == nil || tb == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.settleLocked(tb)
}

type checkpointer interface {
	Set(ctx context.Context, shardID, sequenceNumber, approxCreationTime string) error
	CheckpointLimit() int
}

// advanceFrontier records the resolved contiguous frontier and its associated
// record timestamp, pruning seqTimes entries the frontier has passed.
func (st *shardAckTracker) advanceFrontier(frontier string) {
	st.frontier = frontier
	st.frontierTime = st.seqTimes[frontier]
	for seq := range st.seqTimes {
		if seq <= frontier {
			delete(st.seqTimes, seq)
		}
	}
}

// AckBatch marks a tracked batch as acknowledged, advances the shard's
// contiguous frontier, and persists a checkpoint once enough messages have
// been acked since the last persisted position. Settlement goes through the
// handle returned by AddMessages (see trackedBatch); repeat settles no-op.
func (b *RecordBatcher) AckBatch(
	ctx context.Context,
	cp checkpointer,
	tb *trackedBatch,
) error {
	if b == nil || tb == nil {
		return nil
	}

	// Settle under b.mu only, never waiting on another ack's in-flight
	// checkpoint write: batch settlement (and with it TryReserve and
	// AddMessages) stays wait-free even when the checkpoint store is slow.
	b.mu.Lock()
	if !b.settleLocked(tb) {
		// Already settled (nacked or double-acked); nothing to do.
		b.mu.Unlock()
		return nil
	}
	st := b.shards[tb.shardID]
	if frontier := tb.resolve(); frontier != nil {
		st.advanceFrontier(*frontier)
	}
	st.pending += tb.size
	shardsOverCap := len(b.shards) > b.maxTrackedShards
	b.mu.Unlock()

	if cp == nil {
		// No checkpoint store: settlement still drains the tracker, there is
		// just nothing to persist.
		return nil
	}
	persistErr := b.maybePersist(ctx, cp, st, tb.shardID)
	if shardsOverCap {
		// The batch is settled and the frontier persisted above regardless:
		// swallowing the settle would leak tracked messages and permanently
		// pin ShouldThrottle, and skipping persists would silently freeze
		// durable checkpoints for as long as the guard trips.
		return errors.Join(
			fmt.Errorf("checkpoint map exceeded maximum size (%d shards) - possible memory leak", b.maxTrackedShards),
			persistErr,
		)
	}
	return persistErr
}

// maybePersist writes a shard's checkpoint once enough messages have been
// acked since the last persisted position AND the contiguous frontier has
// moved past it (a pinned frontier - nacked batch ahead in stream order -
// accumulates pending acks without persisting).
//
// Writes are single-flighted per shard: whoever holds persistMu re-checks
// after each successful write, so progress settled during the write is
// picked up immediately, and every other ack skips past without blocking.
// Anything left unpersisted when the writes stop is carried by the shard's
// next ack or the shutdown flush (PendingCheckpoints/FlushCheckpoints).
// b.mu is never held across the store write.
func (b *RecordBatcher) maybePersist(ctx context.Context, cp checkpointer, st *shardAckTracker, shardID string) error {
	if !st.persistMu.TryLock() {
		return nil
	}
	defer st.persistMu.Unlock()

	for {
		b.mu.Lock()
		due := st.pending >= cp.CheckpointLimit() && st.frontier != "" && st.frontier != st.persisted
		persistSeq := st.frontier
		persistTime := st.frontierTime
		pendingAtCompute := st.pending
		b.mu.Unlock()
		if !due {
			return nil
		}

		// Bound the write so a hung request surfaces as a retryable error
		// (the ack context is typically deadline-free) instead of pinning
		// the shard's persist slot indefinitely. An abandoned write can in
		// principle still land server-side after a later one and step the
		// durable row backwards (bounded redelivery after a crash inside
		// that window, never loss). Stream sequence numbers are
		// variable-length numeric strings, so a lexicographic
		// ConditionExpression guarding forward-only movement could wrongly
		// reject legitimate newer writes forever - a frozen checkpoint,
		// strictly worse than the race it would close.
		setCtx, cancel := context.WithTimeout(ctx, defaultAPICallTimeout)
		err := cp.Set(setCtx, shardID, persistSeq, persistTime)
		cancel()
		if err != nil {
			// Bookkeeping untouched: pending keeps accumulating and the next
			// ack on this shard retries the write from the newest frontier.
			return err
		}

		b.mu.Lock()
		st.persisted = persistSeq
		// Subtract only what this write covered; acks settled while it was
		// in flight keep counting toward the next persist.
		st.pending -= pendingAtCompute
		b.mu.Unlock()
		b.log.Debugf("Checkpointed shard %s at sequence %s", shardID, persistSeq)
	}
}

// PendingCheckpoints returns, per shard, the highest contiguous acked
// sequence that has not been persisted yet. Used to flush checkpoints on
// shutdown.
func (b *RecordBatcher) PendingCheckpoints() map[string]CheckpointValue {
	b.mu.Lock()
	defer b.mu.Unlock()

	checkpoints := make(map[string]CheckpointValue, len(b.shards))
	for shardID, st := range b.shards {
		if st.frontier != "" && st.frontier != st.persisted {
			checkpoints[shardID] = CheckpointValue{
				SequenceNumber:     st.frontier,
				ApproxCreationTime: st.frontierTime,
			}
		}
	}
	return checkpoints
}

// ShouldThrottle returns true if the message tracker is near capacity and
// backpressure should be applied.
func (b *RecordBatcher) ShouldThrottle() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// Throttle at 90% capacity to leave some headroom. Readers use
	// TryReserve (which also carries the pinned-throttle diagnostics); this
	// remains for callers that only need a point-in-time pressure signal.
	return b.trackedMessages >= (b.maxTrackedMessages * 9 / 10)
}

// PendingCount returns the count of acked-but-not-persisted messages for a
// shard. Exported for testing.
func (b *RecordBatcher) PendingCount(shardID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, ok := b.shards[shardID]; ok {
		return st.pending
	}
	return 0
}

// TrackedMessageCount returns the number of tracked messages. Exported for testing.
func (b *RecordBatcher) TrackedMessageCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trackedMessages
}

// LastCheckpoint returns the highest contiguous acked sequence for a shard.
// Exported for testing.
func (b *RecordBatcher) LastCheckpoint(shardID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, ok := b.shards[shardID]; ok {
		return st.frontier
	}
	return ""
}

// LastCheckpointsCount returns the number of shards with an unpersisted
// frontier. Exported for testing.
func (b *RecordBatcher) LastCheckpointsCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, st := range b.shards {
		if st.frontier != "" && st.frontier != st.persisted {
			n++
		}
	}
	return n
}
