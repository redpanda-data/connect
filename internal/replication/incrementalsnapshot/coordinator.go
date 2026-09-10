// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// Coordinator backfills a set of tables in key-ordered, key-bounded chunks
// while a replication stream runs, dropping each buffered row the stream has
// already delivered.
//
// It holds only the algorithm: which table and key range to read next, when a
// chunk is safe to emit, and what the caller may checkpoint. Side effects come
// from Deps, window comparisons from Watermark.
//
// Not safe for concurrent use: call OnStreamedRow and OnCommit from a single
// goroutine, in stream order. OnCommit's chunk read blocks on I/O by design.
type Coordinator[P any, W Watermark[P]] struct {
	cfg CoordinatorConfig[P, W]

	// resume holds the state given to NewCoordinator; nil once Start has
	// consumed it.
	resume *State

	remaining []TableID
	current   *TableID
	// currentExhausted marks current's last chunk as fetched, so the next
	// plan advances tables. current stays set until then, keeping the
	// buffered final chunk dedupable. Deliberately not in State: a resume
	// re-issues one empty query for the table and moves on.
	currentExhausted bool
	pkCols           map[string][]string
	maxPK            PrimaryKey
	lastSentPK       PrimaryKey
	low              W
	high             W
	windowOpened     bool
	// idle records an empty queue, so there is nothing to read until
	// AddTables supplies more. It is not terminal: tables arrive at any
	// time, so no completion is ever emitted.
	idle bool
	// queueChanged marks a queue AddTables has extended but no checkpoint
	// has recorded. The next OnCommit emits one before anything else, so the
	// request survives the acknowledgement of whatever carried it.
	queueChanged bool
	// needsPlan marks a coordinator that left idle, so it holds no chunk
	// and no window bounds. The next OnCommit plans before it judges a
	// window: the bounds left from going idle would close an empty one and
	// checkpoint no rows.
	needsPlan bool
	window    *WindowBuffer

	// committed mirrors remaining/current/maxPK/lastSentPK but only advances
	// after a flush. State reports these, so a crash before a flush re-reads
	// the chunk on resume rather than skipping it.
	committedRemaining  []TableID
	committedCurrent    *TableID
	committedMaxPK      PrimaryKey
	committedLastSentPK PrimaryKey

	// knownTables is every table this run covers, the finished ones
	// included. AddTables extends it and State reports it, so a later run
	// can tell a newly requested table from one already backfilled.
	knownTables []TableID
}

// NewCoordinator builds a Coordinator. A non-nil resume makes Start continue
// from that state; otherwise it starts with an empty queue, which AddTables
// fills.
func NewCoordinator[P any, W Watermark[P]](cfg CoordinatorConfig[P, W], resume *State) (*Coordinator[P, W], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxDrainChunks == 0 {
		cfg.MaxDrainChunks = DefaultMaxDrainChunks
	}

	return &Coordinator[P, W]{
		cfg:    cfg,
		resume: resume.Clone(),
		pkCols: make(map[string][]string),
		window: NewWindowBuffer(),
	}, nil
}

// EmitFunc receives one chunk's rows as the coordinator releases them. It is
// called after the committed state has advanced past those rows, so State may
// be read from inside it to get their checkpoint.
//
// It is also the drain's backpressure: the next chunk is only released once
// EmitFunc returns, so a slow consumer costs one buffered chunk. An error
// aborts the operation.
type EmitFunc func(rows []Row) error

// Start must be called once before any other method.
//
// It never emits: it buffers the first chunk and returns, leaving OnCommit to
// release everything. Callers may therefore start the coordinator before
// their downstream is consuming. With nothing queued it goes idle and waits
// for AddTables.
func (c *Coordinator[P, W]) Start(ctx context.Context) error {
	resume := c.resume
	c.resume = nil

	if resume != nil {
		// The checkpoint replaces the queue, so anything AddTables queued
		// before Start is re-applied behind it. AddTables already reported
		// these as queued.
		seeded := c.remaining

		c.current = resume.CurrentTable
		c.lastSentPK = resume.LastSentPK
		c.maxPK = resume.MaxPK
		c.remaining = resume.RemainingTables
		c.knownTables = resume.Tables

		// Through AddTables, so a table the checkpoint already covers is
		// skipped and a resume cannot re-read a finished one.
		c.AddTables(seeded)
	}

	// Anything seeded before now owes no checkpoint: the caller has
	// acknowledged nothing yet, so there is no request that could be lost.
	c.queueChanged = false

	// Baseline: nothing fetched yet. planNextChunk advances past it.
	c.commitLiveState()

	// Once only, so the stream has a known position for the first watermark.
	// See Deps.ForceFreshTransaction for why not per watermark.
	if err := c.cfg.Deps.ForceFreshTransaction(ctx); err != nil {
		return fmt.Errorf("forcing fresh transaction: %w", err)
	}

	// State never captures an unflushed chunk, so resuming always means
	// planning the next one. Watermarks are always re-derived.
	return c.planNextChunk(ctx)
}

// commitLiveState snapshots the live fields into the committed ones. Call
// only once everything fetched so far has been flushed.
func (c *Coordinator[P, W]) commitLiveState() {
	c.committedRemaining = slices.Clone(c.remaining)
	c.committedCurrent = c.current
	c.committedMaxPK = c.maxPK
	c.committedLastSentPK = c.lastSentPK
}

// Idle reports an empty queue, so nothing is being read. AddTables may
// bring more, so this is not a completion signal.
func (c *Coordinator[P, W]) Idle() bool {
	return c.idle
}

// AddTables queues tables for backfill and reports the ones it queued. It
// is the only way a table enters the queue.
//
// Call it at any time. Before Start it seeds the queue; on a resume the
// checkpoint's queue is read first and these follow it. Afterwards the next
// OnCommit checkpoints the queue and plans.
//
// A table is skipped when knownTables holds it, which covers the finished
// ones. Re-reading one takes a fresh checkpoint.
func (c *Coordinator[P, W]) AddTables(tables []TableID) (added []TableID) {
	for _, table := range tables {
		if slices.Contains(c.knownTables, table) {
			continue
		}
		added = append(added, table)
		c.knownTables = append(c.knownTables, table)
		c.remaining = append(c.remaining, table)
		// Also the committed queue, which is what State reports. Nothing
		// has been read yet, so "committed" is the truth here. Without it
		// State reports the table under Tables with no queue entry, and a
		// resume from that is idle and rejects a repeat request -- the
		// table could never be read.
		c.committedRemaining = append(c.committedRemaining, table)
	}
	if len(added) > 0 {
		// Owed whether or not a backfill is already running: a signal that
		// arrives mid-backfill is just as unrepeatable as one that arrives
		// idle, and the running table's window may not close for a while.
		c.queueChanged = true
	}
	if len(added) > 0 && c.idle {
		c.idle = false
		c.needsPlan = true
	}
	return added
}

// OnStreamedRow must be cheap and do no I/O. It removes pk from the buffer
// only while that table is being snapshotted; otherwise it is a no-op.
//
// Known limitation: a row can be delivered twice when an insert reuses a key
// below MaxPK and streams in before the covering chunk is read. Monotonic
// keys (serial, UUIDv7) cannot hit this. Consumers should treat rows as
// idempotent upserts by key, as standard CDC practice.
func (c *Coordinator[P, W]) OnStreamedRow(table TableID, pk PrimaryKey) (removed bool) {
	if c.idle || c.current == nil || table != *c.current {
		return false
	}
	return c.window.Remove(table, pk)
}

// OnCommit is called once per completed transaction, in stream order.
//
// Pass only a position the database actually reported. Filter unknown ones
// first: a zero value standing in for a missing BEGIN can open or close the
// window spuriously.
func (c *Coordinator[P, W]) OnCommit(ctx context.Context, pos P, emit EmitFunc) (changed bool, err error) {
	if c.idle {
		// Nothing queued or buffered, so this commit has no bearing on the
		// snapshot. AddTables clears it.
		return false, nil
	}

	if c.queueChanged {
		// Checkpoint the extended queue before anything else. The request
		// for those tables belongs to this transaction, so once the caller
		// acknowledges this position it is gone: nothing replays it, and a
		// restart would find the tables unqueued.
		//
		// State reports the committed queue, which AddTables has already
		// extended, and the window holds nothing new, so this emits state
		// alone.
		c.queueChanged = false
		if err := emit(nil); err != nil {
			// Not delivered, so the queue is still owed a checkpoint.
			c.queueChanged = true
			return false, err
		}

		if c.needsPlan {
			// The queue was empty until now, so buffer a chunk and let the
			// following commit close its window, the normal cadence.
			c.needsPlan = false
			return true, c.planNextChunk(ctx)
		}
		// A backfill is already running: fall through so this commit is
		// still judged against its window. The checkpoint above already
		// counts as a change, whatever the window does.
		changed = true
	}

	if !c.windowOpened && c.low.OpensAt(pos) {
		c.windowOpened = true
	}

	// Both watermarks must be clear of pos: they bracket the chunk read, so
	// the later of the two is the real bound.
	if !c.windowOpened || !c.low.ClosesAt(pos) || !c.high.ClosesAt(pos) {
		return changed, nil
	}

	if err := c.releaseWindow(emit); err != nil {
		return false, err
	}
	return true, c.planAndDrain(ctx, emit)
}

// releaseWindow hands the buffered chunk to emit, advancing the committed
// state first so State reports the right checkpoint from inside emit.
//
// emit is what delivers the rows, so an error from it means nothing was
// sent. The advance is then undone and the rows go back in the buffer:
// State must never report a chunk as checkpointed that no consumer
// received, or a resume from that State would fetch past those rows and
// drop them.
func (c *Coordinator[P, W]) releaseWindow(emit EmitFunc) error {
	rows := c.window.Flush()
	undo := c.snapshotCommitted()
	wasOpened := c.windowOpened

	c.windowOpened = false
	c.commitLiveState()

	if err := emit(rows); err != nil {
		c.restoreCommitted(undo)
		c.windowOpened = wasOpened
		for _, row := range rows {
			c.window.Add(row)
		}
		return err
	}
	return nil
}

// committedState is the checkpoint half of the coordinator, saved so a
// failed emit can be undone.
type committedState struct {
	remaining  []TableID
	current    *TableID
	maxPK      PrimaryKey
	lastSentPK PrimaryKey
}

func (c *Coordinator[P, W]) snapshotCommitted() committedState {
	return committedState{
		remaining:  c.committedRemaining,
		current:    c.committedCurrent,
		maxPK:      c.committedMaxPK,
		lastSentPK: c.committedLastSentPK,
	}
}

func (c *Coordinator[P, W]) restoreCommitted(s committedState) {
	c.committedRemaining = s.remaining
	c.committedCurrent = s.current
	c.committedMaxPK = s.maxPK
	c.committedLastSentPK = s.lastSentPK
}

// planAndDrain buffers the next chunk, then keeps releasing and replanning
// while each chunk came from an undisturbed read.
//
// An equal, quiesced watermark pair proves nothing could have modified the
// chunk during the read, so no streamed row can supersede it and there is
// nothing to wait for. Holding it would mean waiting for a commit that, on a
// quiet table, only arrives with the next heartbeat -- which is what
// otherwise caps the backfill at one chunk per heartbeat.
//
// Capped at cfg.MaxDrainChunks per call: emitting runs on the caller's
// replication loop, which usually also owes standby keepalives, so an
// unbounded drain risks the server dropping the connection. Progress resumes
// on the next commit.
func (c *Coordinator[P, W]) planAndDrain(ctx context.Context, emit EmitFunc) error {
	for range c.cfg.MaxDrainChunks {
		if err := c.planNextChunk(ctx); err != nil {
			return err
		}
		if c.idle || !c.readUndisturbed() {
			return nil
		}
		if err := c.releaseWindow(emit); err != nil {
			return err
		}
	}
	return c.planNextChunk(ctx)
}

// readUndisturbed reports whether the buffered chunk's watermarks prove a
// still database: nothing in flight at either end, and nothing assigned
// between them.
func (c *Coordinator[P, W]) readUndisturbed() bool {
	return c.low == c.high && c.low.Quiesced()
}

// State returns the resumable state; safe to call any time after Start. It
// reports the committed fields.
func (c *Coordinator[P, W]) State() *State {
	s := &State{
		Version:         CurrentStateVersion,
		CurrentTable:    c.committedCurrent,
		LastSentPK:      c.committedLastSentPK,
		MaxPK:           c.committedMaxPK,
		RemainingTables: c.committedRemaining,
		Tables:          c.knownTables,
	}
	return s.Clone()
}

// dropUnusable removes the current table from the run when err reports it
// can never be backfilled -- refer to ErrTableUnusable for why dropping
// beats failing. Reports whether it dropped anything.
//
// The table stays in knownTables, so a later request for it is a no-op
// until the caller starts from a fresh checkpoint. Re-reading it would fail
// the same way.
func (c *Coordinator[P, W]) dropUnusable(table TableID, err error) bool {
	if !errors.Is(err, ErrTableUnusable) {
		return false
	}
	if c.cfg.OnTableDropped != nil {
		c.cfg.OnTableDropped(table, err)
	}
	c.current = nil
	c.currentExhausted = false
	c.lastSentPK = nil
	c.maxPK = nil
	return true
}

// planNextChunk buffers the current table's next chunk, advancing tables
// until one has rows or none remain.
func (c *Coordinator[P, W]) planNextChunk(ctx context.Context) error {
	for {
		if c.current == nil || c.currentExhausted {
			if len(c.remaining) == 0 {
				c.current = nil
				c.currentExhausted = false
				c.idle = true
				return nil
			}

			next := c.remaining[0]
			c.remaining = c.remaining[1:]
			c.current = &next
			c.currentExhausted = false
			c.lastSentPK = nil
			c.maxPK = nil
		}

		table := *c.current

		pkCols, err := c.resolvePKCols(ctx, table)
		if err != nil {
			if c.dropUnusable(table, err) {
				continue
			}
			return err
		}

		if err := c.resolveMaxPK(ctx, table, pkCols); err != nil {
			if c.dropUnusable(table, err) {
				continue
			}
			return err
		}

		if c.maxPK == nil {
			// Empty table: treat like an exhausted one rather than
			// failing the whole coordinator.
			c.current = nil
			continue
		}

		low, err := c.resolveWatermark(ctx)
		if err != nil {
			return err
		}

		rows, err := c.cfg.Deps.FetchChunk(ctx, table, pkCols, c.lastSentPK, c.maxPK, c.cfg.ChunkSize)
		if err != nil {
			return fmt.Errorf("fetching chunk for table %s: %w", table, err)
		}

		high, err := c.resolveWatermark(ctx)
		if err != nil {
			return err
		}

		c.low = low
		c.high = high

		if len(rows) == 0 {
			// Exhausted; keep looping until a table has rows or none
			// remain.
			c.current = nil
			continue
		}

		for _, row := range rows {
			c.window.Add(row)
		}
		c.lastSentPK = rows[len(rows)-1].PK

		if len(rows) < c.cfg.ChunkSize {
			// Final, partial chunk. Mark the table exhausted so the next
			// plan skips a wasted empty query, but leave c.current set:
			// these rows are still buffered, and OnStreamedRow can only
			// dedup them while current names their table. Clearing it
			// would let a concurrent update stream past undeduped and be
			// overwritten by this stale chunk on flush.
			c.currentExhausted = true
		}

		return nil
	}
}

func (c *Coordinator[P, W]) resolvePKCols(ctx context.Context, table TableID) ([]string, error) {
	key := table.String()
	if cols, exists := c.pkCols[key]; exists {
		return cols, nil
	}

	cols, err := c.cfg.Deps.ResolvePrimaryKey(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("resolving primary key columns for table %s: %w", table, err)
	}
	c.pkCols[key] = cols
	return cols, nil
}

// resolveMaxPK resolves and caches the table's max key, leaving c.maxPK nil
// for an empty table -- which planNextChunk treats as nothing to backfill.
func (c *Coordinator[P, W]) resolveMaxPK(ctx context.Context, table TableID, pkCols []string) error {
	if c.maxPK != nil {
		return nil
	}

	maxPK, err := c.cfg.Deps.ResolveMaxKey(ctx, table, pkCols)
	if err != nil {
		return fmt.Errorf("resolving max key for table %s: %w", table, err)
	}
	c.maxPK = maxPK
	return nil
}

// resolveWatermark reads a watermark. It deliberately forces no transaction
// first: each read is its own statement and already sees a current snapshot,
// and assigning an id here would make every chunk's pair differ, which
// readUndisturbed relies on not happening.
func (c *Coordinator[P, W]) resolveWatermark(ctx context.Context) (W, error) {
	var zero W
	wm, err := c.cfg.Deps.ResolveWatermark(ctx)
	if err != nil {
		return zero, fmt.Errorf("resolving watermark: %w", err)
	}
	return wm, nil
}
