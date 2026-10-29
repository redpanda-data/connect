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

// pinClock encapsulates continuous throttling duration tracking and
// rate-limited warning log state.
type pinClock struct {
	// throttledSince records when reservations started continuously failing
	// without an intervening successful reservation/check; zero while not
	// throttled.
	throttledSince time.Time
	// lastPinWarn is the timestamp when the pinned-throttle warning was last
	// logged, used to rate-limit warnings.
	lastPinWarn time.Time
}

// clear ends the current pin. It keeps lastPinWarn on purpose, so a pin that
// flaps (clears and starts again) stays rate-limited.
func (c *pinClock) clear() {
	c.throttledSince = time.Time{}
}

// check records the start of a throttling event or checks if a warning is due.
// Returns the total duration spent continuously throttled and whether a log
// warning should be emitted.
func (c *pinClock) check(now time.Time) (since time.Duration, shouldWarn bool) {
	if c.throttledSince.IsZero() {
		c.throttledSince = now
		return 0, false
	}
	since = now.Sub(c.throttledSince)
	if since >= throttlePinWarnAfter && now.Sub(c.lastPinWarn) >= throttlePinWarnInterval {
		c.lastPinWarn = now
		return since, true
	}
	return since, false
}

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
//
// All writes to the checkpoint store go through one persister per batcher
// (Run). The trackers hold the wanted state of every shard (its frontier,
// the frontier's record time, whether the shard is closed and settled) next
// to the persisted state, and the persister writes the difference: acks and
// closes mark a shard dirty and wake it, a failed write stays dirty and is
// retried with backoff, and Flush writes everything left at shutdown. Acks
// therefore never wait on the store and never see a write error, and there
// is exactly one place a failed write is owned.
type RecordBatcher struct {
	maxTrackedShards   int
	maxTrackedMessages int
	log                *service.Logger
	// cp is the checkpoint store every write goes to. It is nil only in
	// tests: finished shards are then forgotten without a write and open
	// shards never persist.
	cp checkpointer
	// failures counts failed checkpoint writes (nil-safe).
	failures *service.MetricCounter
	// retryInitial and retryMax bound the persister's retry backoff
	// (checkpoint_retry_initial_interval/checkpoint_retry_max_interval).
	retryInitial, retryMax time.Duration

	// wake is signalled (non-blocking, buffered 1) whenever a shard becomes
	// dirty, so the persister runs a pass; a pass covers every dirty shard,
	// so one buffered signal is enough.
	wake chan struct{}
	// writeMu serializes persister passes: the Run loop and a Flush at
	// shutdown must never write concurrently, or a late write could step a
	// shard's row backwards. Never held together with mu across a write.
	writeMu sync.Mutex

	mu sync.Mutex
	// dirty holds the shards with a wanted state the persister has not
	// matched yet: an open shard due a checkpoint, a closed and settled shard
	// awaiting its final write, or a finished shard still to be forgotten. A
	// shard stays here until its write lands.
	dirty map[string]struct{}

	// shards holds the per-shard ordered ack trackers. An entry is removed
	// once its closed shard has been read to the end and every batch from it
	// has settled (see CloseShard), or when shard discovery forgets a shard
	// that aged out of the stream (see ForgetShard). Without that, every
	// rotation (a partition's shard closes roughly every four hours) would
	// leave an entry behind, and past maxTrackedShards every ack would fail.
	shards map[string]*shardAckTracker
	// trackedMessages counts messages across all in-flight batches, used for
	// backpressure via ShouldThrottle.
	trackedMessages int
	// reserved counts budget handed out via TryReserve but not yet converted
	// into tracked messages (reads in flight). trackedMessages + reserved is
	// bounded by maxTrackedMessages, so concurrent readers can never
	// collectively overshoot the budget on their first read wave.
	reserved int
	// reservedByShard breaks reserved down per shard, for the per-shard cap
	// and so Release/AddMessages can return or consume the right shard's
	// budget. Kept outside shards and pruned at zero: shards entries count
	// against maxTrackedShards and live until the shard is finished, so a
	// reader polling a shard that never yields records must not materialise
	// tracker state.
	reservedByShard map[string]int
	// perShardCap bounds one shard's in-flight (tracked + reserved) messages
	// so a shard whose batches never settle downstream parks alone at its cap
	// instead of pinning the global budget and every other reader. The cap
	// exists purely for that isolation, so it binds only while another shard
	// is also active: a sole active shard may use the whole global budget (a
	// one-partition table has a single active shard, and capping it would
	// strand three quarters of the budget the pre-cap gate allowed it). A
	// shard with nothing in flight always admits one batch regardless, so
	// progress is guaranteed for any cap/batch-size combination.
	perShardCap int
	// pinClock tracks continuous throttling duration and rate-limits pinned
	// warnings on the global budget.
	pinClock pinClock
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
	tracker *checkpoint.Uncapped[string]
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
	// inflight counts this shard's tracked (dispatched, unsettled) messages.
	// Together with the shard's entry in the batcher's reservedByShard it is
	// bounded by perShardCap.
	inflight int
	// pinClock tracks continuous throttling duration and rate-limits pinned
	// warnings on this shard's in-flight cap.
	pinClock pinClock

	// lastTracked is the checkpoint sequence of the most recently tracked
	// batch. A closed shard whose frontier reached it had every dispatched
	// batch acknowledged, so it may be marked completed.
	lastTracked string
	// closed is set by CloseShard once the shard's reader has dispatched its
	// final batch. done is set once the persister has written the shard's
	// final position (see CloseShard); the entry is then only waiting to be
	// forgotten and nothing is written for it again.
	closed bool
	done   bool
}

func newShardAckTracker() *shardAckTracker {
	return &shardAckTracker{tracker: checkpoint.NewUncapped[string](), seqTimes: map[string]string{}}
}

// NewRecordBatcher creates a new [RecordBatcher] for DynamoDB CDC whose
// checkpoints are written to cp. failures, when not nil, counts failed
// checkpoint writes. The persister must be started with Run.
func NewRecordBatcher(maxTrackedShards, checkpointLimit int, cp checkpointer, failures *service.MetricCounter, log *service.Logger) *RecordBatcher {
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
		perShardCap:     maxTrackedMessages / 4,
		log:             log,
		cp:              cp,
		failures:        failures,
		retryInitial:    persistRetryInitial,
		retryMax:        persistRetryMax,
		wake:            make(chan struct{}, 1),
		shards:          make(map[string]*shardAckTracker),
		dirty:           make(map[string]struct{}),
		reservedByShard: make(map[string]int),
	}
}

// TryReserve claims budget for a read of up to n messages on shardID. It
// refuses when the global tracked+reserved budget or the shard's in-flight
// cap has no room (the caller waits and retries). The per-shard cap binds
// only while another shard is also active, and a shard with nothing in
// flight always admits one batch, so progress is never impossible. Budget is
// consumed by AddMessages and any surplus must be returned via Release.
func (b *RecordBatcher) TryReserve(shardID string, n int) bool {
	if b == nil {
		// No batcher, no backpressure (mirrors ShouldThrottle's nil case).
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// If the batcher has zero messages in flight, always allow the first
	// reservation to go through, even if it exceeds the maximum budget.
	//
	// Why? Even though configuration validation normally ensures batch sizes
	// are smaller than the budget, a batch that is larger than the limit must
	// never cause the reader loop to hang forever waiting for space that can
	// never clear.
	globalInFlight := b.inFlightCountLocked()
	if globalInFlight > 0 && globalInFlight+n > b.maxTrackedMessages {
		// Surface a continuously pinned budget: every shard reader parks on
		// this check, so if in-flight messages never settle the input is
		// stalled with no other signal.
		now := time.Now()
		if since, shouldWarn := b.pinClock.check(now); shouldWarn {
			b.log.Warnf("Shard readers throttled for %v: %d/%d in-flight messages are still awaiting downstream acknowledgement (top shards: %s); no records are being read while this persists",
				since.Round(time.Second), b.trackedMessages, b.maxTrackedMessages, b.topInflightShardsLocked(3))
		}
		return false
	}
	// The global budget can admit this reservation, so it is not pinned:
	// clear its clock even if the shard's own cap refuses below, or the next
	// global exhaustion would report a pin spanning the drained interval.
	b.pinClock.clear()

	shardInFlight := b.shardInFlightLocked(shardID)
	if shardInFlight > 0 && shardInFlight+n > b.perShardCap && b.otherShardActiveLocked(shardID) {
		// The shard is pinned by its own unsettled messages; park it alone
		// without touching the global throttle clock, but surface a
		// continuous pin on the shard's own clock - in a topology with fewer
		// than four pinned shards the global branch above never trips, so
		// this is the only place a wedged shard can become visible. A pinned
		// shard always has tracked messages (its single reader never holds a
		// reservation while reserving again), so st is non-nil here; the
		// guard is belt and braces.
		if st := b.shards[shardID]; st != nil {
			now := time.Now()
			if since, shouldWarn := st.pinClock.check(now); shouldWarn {
				b.log.Warnf("Shard %s reader throttled for %v: %d/%d in-flight messages on this shard are still awaiting downstream acknowledgement; no records are being read from it while this persists",
					shardID, since.Round(time.Second), shardInFlight, b.perShardCap)
			}
		}
		return false
	}

	if st := b.shards[shardID]; st != nil {
		st.pinClock.clear()
	}
	b.reservedByShard[shardID] += n
	b.reserved += n
	return true
}

// otherShardActiveLocked reports whether any shard other than shardID has
// unsettled (in-flight) messages - the per-shard cap only binds while one
// does (see perShardCap doc). A transient read reservation is deliberately
// not activity: idle shards hold one for every GetRecords round trip, and
// counting those would intermittently engage the cap against a sole busy
// shard. Callers must hold b.mu.
func (b *RecordBatcher) otherShardActiveLocked(shardID string) bool {
	for id, st := range b.shards {
		if id != shardID && st.inflight > 0 {
			return true
		}
	}
	return false
}

// consumeReservationLocked removes up to n from a shard's outstanding
// reservation, pruning the entry at zero, and returns how much was consumed.
// Callers must hold b.mu.
func (b *RecordBatcher) consumeReservationLocked(shardID string, n int) int {
	consumed := min(n, b.reservedByShard[shardID])
	if consumed == 0 {
		return 0
	}
	if rem := b.reservedByShard[shardID] - consumed; rem > 0 {
		b.reservedByShard[shardID] = rem
	} else {
		delete(b.reservedByShard, shardID)
	}
	b.reserved -= consumed
	return consumed
}

// Release returns unused reservation for a shard (the read failed, returned
// fewer records than reserved, or every record was filtered out).
func (b *RecordBatcher) Release(shardID string, n int) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consumeReservationLocked(shardID, n)
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
	// The nil guard matters at shutdown: Close() nils the input's batcher
	// reference after timeouts that warn and proceed, and a straggler reader
	// returning from a slow GetRecords must no-op (all other methods already
	// tolerate nil; every caller tolerates a nil handle), not panic and kill
	// the process.
	if b == nil || len(batch) == 0 {
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
		st = newShardAckTracker()
		b.shards[shardID] = st
	}

	// Convert the shard's outstanding reservation (if any) into tracked
	// messages; readers reserve at least the batch size before reading.
	b.consumeReservationLocked(shardID, len(batch))

	last := batch[len(batch)-1]
	seq, _ := last.MetaGet("dynamodb_sequence_number")
	approxCreationTime, _ := last.MetaGet("dynamodb_approximate_creation_time")
	st.seqTimes[seq] = approxCreationTime
	st.lastTracked = seq
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
// it and a restart redelivers the dropped records. Dropping the last batch of
// a closed shard hands the shard to the persister (see CloseShard).
func (b *RecordBatcher) RemoveBatch(tb *trackedBatch) {
	if b == nil || tb == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.settleLocked(tb) {
		return
	}
	if st := b.shards[tb.shardID]; st != nil && st.closed && st.inflight == 0 {
		b.markDirtyLocked(tb.shardID)
	}
}

type checkpointer interface {
	Set(ctx context.Context, shardID, sequenceNumber, approxCreationTime string) error
	MarkShardCompleted(ctx context.Context, shardID, sequenceNumber, approxCreationTime string) error
	CheckpointLimit() int
}

// CloseShard records that shardID's reader has read its closed shard to the
// end and dispatched its final batch. Once every tracked batch on the shard
// has settled (immediately if none is in flight), the persister writes the
// shard's final position:
//
//   - every dispatched batch acked (or nothing was delivered from the shard
//     this run): the frontier is persisted with the completion marker
//     (cp.MarkShardCompleted), so a restart skips the shard instead of
//     re-reading it from a lagging checkpoint;
//   - a batch was dropped (nacked) and pinned the frontier: only the pinned
//     frontier is persisted, so a restart still redelivers the dropped
//     records;
//
// and forgets the shard's tracker entry. A failed write keeps the entry and
// is retried by the persister until it lands, the shutdown Flush writes it,
// or the shard ages out of the stream (ForgetShard).
func (b *RecordBatcher) CloseShard(shardID string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st, exists := b.shards[shardID]
	if !exists {
		// Nothing was delivered from the shard during this run (empty or
		// fully filtered final pages): an empty tracker records the close,
		// so the marker alone is written and the entry forgotten.
		st = newShardAckTracker()
		b.shards[shardID] = st
	}
	st.closed = true
	if st.inflight == 0 {
		b.markDirtyLocked(shardID)
	}
}

// markDirtyLocked queues shardID for the persister and wakes it. Callers
// must hold b.mu.
func (b *RecordBatcher) markDirtyLocked(shardID string) {
	b.dirty[shardID] = struct{}{}
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// Default persister retry backoff: a failed pass is retried after
// persistRetryInitial, doubling up to persistRetryMax while the store keeps
// failing. Waking acks do not cut the wait short. The input overrides both
// from checkpoint_retry_initial_interval and checkpoint_retry_max_interval.
const (
	persistRetryInitial = time.Second
	persistRetryMax     = 30 * time.Second
)

// Run is the batcher's persister: it writes the dirty shards to the
// checkpoint store whenever an ack or close wakes it and retries failed
// writes with backoff, until ctx is done. Run it in one goroutine per
// batcher. Whatever is left unpersisted when it stops is written by Flush.
func (b *RecordBatcher) Run(ctx context.Context) {
	if b == nil {
		return
	}
	backoff := b.retryInitial
	var retry <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.wake:
			if retry != nil {
				// A failed write is waiting out its backoff; the retry pass
				// covers whatever just became dirty as well.
				continue
			}
		case <-retry:
			retry = nil
		}
		if err := b.persist(ctx, false); err != nil {
			if ctx.Err() != nil {
				// Shutting down: Flush takes over what is left.
				return
			}
			b.log.Warnf("Failed to persist checkpoints, retrying in %v: %v", backoff, err)
			retry = time.After(backoff)
			backoff = min(backoff*2, b.retryMax)
			continue
		}
		backoff = b.retryInitial
	}
}

// Flush writes every unpersisted frontier and every pending final position
// to the checkpoint store, ignoring checkpoint_limit. It is called at
// shutdown once Run has stopped; writes are serialized with Run regardless.
func (b *RecordBatcher) Flush(ctx context.Context) error {
	if b == nil {
		return nil
	}
	return b.persist(ctx, true)
}

// shardWrite is one store write planned by the persister for a shard.
type shardWrite struct {
	shardID string
	st      *shardAckTracker
	// final is a closed, settled shard's last write; the entry is marked done
	// and forgotten once it lands. complete writes the completion marker
	// (MarkShardCompleted) instead of a plain checkpoint (Set).
	final, complete bool
	seq, seqTime    string
	persisted       string
	// pending is the shard's pending count when the write was planned; only
	// that much is subtracted once it lands, so acks settled during the
	// write keep counting toward the next checkpoint.
	pending int
}

// persist runs one persister pass: it plans the writes for the dirty shards
// (every shard when force is set, ignoring checkpoint_limit), performs them
// outside b.mu, and records what landed. A shard whose write fails stays
// dirty; the pass carries on with the other shards and reports the joined
// errors. Each write is bounded by defaultAPICallTimeout.
func (b *RecordBatcher) persist(ctx context.Context, force bool) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	writes, finished := b.planWrites(force)
	for _, shardID := range finished {
		b.forgetFinishedShard(shardID)
	}
	var errs error
	for _, w := range writes {
		if err := ctx.Err(); err != nil {
			b.requeue(w.shardID)
			errs = errors.Join(errs, err)
			continue
		}
		if err := b.write(ctx, w, force); err != nil {
			b.requeue(w.shardID)
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// planWrites takes the dirty shards (all shards when force is set) off the
// dirty set and decides what each one needs: a write, forgetting (its final
// write already landed), or nothing yet. A shard an ack dirties again while
// its write is in flight re-enters the set and is picked up by the next pass.
func (b *RecordBatcher) planWrites(force bool) (writes []shardWrite, finished []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var limit int
	if b.cp != nil {
		limit = b.cp.CheckpointLimit()
	}
	var candidates []string
	if force {
		for shardID := range b.shards {
			candidates = append(candidates, shardID)
		}
	} else {
		for shardID := range b.dirty {
			candidates = append(candidates, shardID)
		}
	}
	sort.Strings(candidates)

	for _, shardID := range candidates {
		delete(b.dirty, shardID)
		st := b.shards[shardID]
		switch {
		case st == nil:
		case st.done:
			finished = append(finished, shardID)
		case st.closed && st.inflight == 0:
			writes = append(writes, shardWrite{
				shardID: shardID, st: st, final: true,
				complete: st.frontier == st.lastTracked,
				seq:      st.frontier, seqTime: st.frontierTime, persisted: st.persisted, pending: st.pending,
			})
		case b.cp != nil && st.hasUnpersistedFrontier() && (force || st.pending >= limit):
			writes = append(writes, shardWrite{
				shardID: shardID, st: st,
				seq: st.frontier, seqTime: st.frontierTime, persisted: st.persisted, pending: st.pending,
			})
		}
	}
	return writes, finished
}

// requeue puts a shard whose write did not land back on the dirty set.
func (b *RecordBatcher) requeue(shardID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.shards[shardID]; exists {
		b.dirty[shardID] = struct{}{}
	}
}

// write performs one planned write and records it. cp is nil only in tests:
// a final write then forgets the entry without touching a store.
//
// A plain write abandoned on timeout can still land server-side after a
// later one and step the durable row backwards (bounded redelivery after a
// crash inside that window, never loss). Stream sequence numbers are
// variable-length numeric strings, so a lexicographic ConditionExpression
// guarding forward-only movement could wrongly reject legitimate newer
// writes forever - a frozen checkpoint, strictly worse than the race it would
// close. What a late plain write can never do is replace the shard's
// completion marker: the store rejects it (see Checkpointer.Set).
func (b *RecordBatcher) write(ctx context.Context, w shardWrite, force bool) error {
	if b.cp != nil {
		var err error
		switch {
		case w.complete:
			err = callStore(ctx, func(ctx context.Context) error {
				return b.cp.MarkShardCompleted(ctx, w.shardID, w.seq, w.seqTime)
			})
		case w.seq != "" && w.seq != w.persisted:
			err = callStore(ctx, func(ctx context.Context) error {
				return b.cp.Set(ctx, w.shardID, w.seq, w.seqTime)
			})
		}
		if err != nil {
			if ctx.Err() == nil {
				// Not a shutdown: a write the store refused.
				b.failures.Incr(1)
			}
			return err
		}
	}

	b.mu.Lock()
	w.st.persisted = w.seq
	// Subtract only what this write covered; acks settled while it was in
	// flight keep counting toward the next checkpoint.
	w.st.pending = max(0, w.st.pending-w.pending)
	if w.final {
		w.st.done = true
	}
	b.mu.Unlock()

	switch {
	case w.final && w.complete:
		b.log.Debugf("Shard %s completed at sequence %s", w.shardID, w.seq)
	case w.final:
		b.log.Debugf("Closed shard %s checkpointed at pinned sequence %s", w.shardID, w.seq)
	case force:
		b.log.Infof("Flushed checkpoint for shard %s at sequence %s", w.shardID, w.seq)
	default:
		b.log.Debugf("Checkpointed shard %s at sequence %s", w.shardID, w.seq)
	}
	if w.final {
		b.forgetFinishedShard(w.shardID)
	}
	return nil
}

// callStore runs one store write under the API call timeout.
func callStore(ctx context.Context, fn func(context.Context) error) error {
	setCtx, cancel := context.WithTimeout(ctx, defaultAPICallTimeout)
	defer cancel()
	return fn(setCtx)
}

// forgetFinishedShard drops a done shard's entry. It cannot normally fail (a
// finished shard has nothing in flight or reserved), but if it does the
// shard is requeued so a later pass drops it.
func (b *RecordBatcher) forgetFinishedShard(shardID string) {
	if !b.ForgetShard(shardID) {
		b.requeue(shardID)
	}
}

// ForgetShard drops shardID's tracker entry once nothing is in flight or
// reserved on it and its frontier is persisted, so finished shards do not
// accumulate against maxTrackedShards. It reports whether the entry is gone
// (true also when there was none).
func (b *RecordBatcher) ForgetShard(shardID string) bool {
	return b.forgetShard(shardID, true)
}

// forgetShard is ForgetShard; requirePersisted false also drops an entry
// whose frontier was never persisted (the shard aged out of the stream, so
// its checkpoint can no longer be used).
func (b *RecordBatcher) forgetShard(shardID string, requirePersisted bool) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st, exists := b.shards[shardID]
	if !exists {
		return true
	}
	if st.inflight > 0 || b.reservedByShard[shardID] > 0 || (requirePersisted && st.frontier != st.persisted) {
		return false
	}
	delete(b.shards, shardID)
	delete(b.dirty, shardID)
	return true
}

// TrackedShardCount returns the number of shards holding tracker state.
// Exported for testing.
func (b *RecordBatcher) TrackedShardCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.shards)
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

// hasUnpersistedFrontier reports whether the shard has a contiguous acked
// sequence frontier that has not yet been persisted.
func (st *shardAckTracker) hasUnpersistedFrontier() bool {
	return st.frontier != "" && st.frontier != st.persisted
}

// AckBatch marks a tracked batch as acknowledged and advances the shard's
// contiguous frontier. Once enough messages have been acked since the last
// persisted position (checkpoint_limit), or the batch was the last one in
// flight on a closed shard, the shard is handed to the persister (Run); the
// ack itself never waits on the store and never sees a write error.
// Settlement goes through the handle returned by AddMessages (see
// trackedBatch); repeat settles no-op.
//
// The only error is the max_tracked_shards guard. The batch is settled and
// the shard queued for persistence regardless: swallowing the settle would
// leak tracked messages and permanently pin ShouldThrottle, and skipping the
// persist would silently freeze durable checkpoints for as long as the guard
// trips.
func (b *RecordBatcher) AckBatch(tb *trackedBatch) error {
	if b == nil || tb == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.settleLocked(tb) {
		// Already settled (nacked or double-acked); nothing to do.
		return nil
	}
	st := b.shards[tb.shardID]
	if frontier := tb.resolve(); frontier != nil {
		st.advanceFrontier(*frontier)
	}
	st.pending += tb.size

	var limit int
	if b.cp != nil {
		limit = b.cp.CheckpointLimit()
	}
	if (st.closed && st.inflight == 0) || (st.hasUnpersistedFrontier() && st.pending >= limit) {
		b.markDirtyLocked(tb.shardID)
	}
	if len(b.shards) > b.maxTrackedShards {
		return fmt.Errorf("checkpoint map exceeded maximum size (%d shards) - possible memory leak", b.maxTrackedShards)
	}
	return nil
}

// PendingCheckpoints returns, per shard, the highest contiguous acked
// sequence that has not been persisted yet (what a Flush would write).
// Exported for testing and diagnostics.
func (b *RecordBatcher) PendingCheckpoints() map[string]CheckpointValue {
	b.mu.Lock()
	defer b.mu.Unlock()

	checkpoints := make(map[string]CheckpointValue, len(b.shards))
	for shardID, st := range b.shards {
		if st.hasUnpersistedFrontier() {
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

// inFlightCountLocked returns the total message load across both stages of
// ingestion:
//
//  1. Reserved (b.reserved): Messages currently being fetched over the
//     network (GetRecords in flight).
//  2. Tracked (b.trackedMessages): Messages received into memory and
//     awaiting downstream processing/acknowledgement.
//
// Counting pending reads as in-flight ensures concurrent shard readers cannot
// flood the pipeline before the first batch of records even arrives.
//
// Callers must hold b.mu.
func (b *RecordBatcher) inFlightCountLocked() int {
	return b.trackedMessages + b.reserved
}

// InFlightCount returns the total number of in-flight (tracked and reserved)
// messages. Exported for testing and diagnostics.
func (b *RecordBatcher) InFlightCount() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlightCountLocked()
}

// shardInFlightLocked returns the in-flight (tracked and reserved) message
// count for a specific shard. Callers must hold b.mu.
func (b *RecordBatcher) shardInFlightLocked(shardID string) int {
	inflight := 0
	if st := b.shards[shardID]; st != nil {
		inflight = st.inflight
	}
	return inflight + b.reservedByShard[shardID]
}

// ShardInFlightCount returns the in-flight (tracked and reserved) message count
// for a specific shard. Exported for testing and diagnostics.
func (b *RecordBatcher) ShardInFlightCount(shardID string) int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.shardInFlightLocked(shardID)
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
		if st.hasUnpersistedFrontier() {
			n++
		}
	}
	return n
}
