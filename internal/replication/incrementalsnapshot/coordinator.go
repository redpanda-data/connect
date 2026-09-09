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
	done             bool
	// doneEmitted records that the terminal Done checkpoint has been handed
	// to emit. Until it is persisted a resume re-resolves every table's key
	// bounds and re-issues an empty chunk query, and never reports complete.
	doneEmitted bool
	window      *WindowBuffer

	// committed mirrors remaining/current/maxPK/lastSentPK but only advances
	// after a flush. State reports these, so a crash before a flush re-reads
	// the chunk on resume rather than skipping it.
	committedRemaining  []TableID
	committedCurrent    *TableID
	committedMaxPK      PrimaryKey
	committedLastSentPK PrimaryKey
}

// NewCoordinator builds a Coordinator. A non-nil resume makes Start continue
// from that state; otherwise it starts fresh from cfg.Tables.
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
// their downstream is consuming. A resume that finds nothing left completes
// here, and the first OnCommit then releases the terminal checkpoint.
func (c *Coordinator[P, W]) Start(ctx context.Context) error {
	resume := c.resume
	c.resume = nil

	if resume == nil {
		c.remaining = slices.Clone(c.cfg.Tables)
	} else {
		c.done = resume.Done
		c.current = resume.CurrentTable
		c.lastSentPK = resume.LastSentPK
		c.maxPK = resume.MaxPK
		c.remaining = resume.RemainingTables

		if c.done {
			return nil
		}
	}

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

// Done reports whether every configured table is fully snapshotted.
func (c *Coordinator[P, W]) Done() bool {
	return c.done
}

// OnStreamedRow must be cheap and do no I/O. It removes pk from the buffer
// only while that table is being snapshotted; otherwise it is a no-op.
//
// Known limitation: a row can be delivered twice when an insert reuses a key
// below MaxPK and streams in before the covering chunk is read. Monotonic
// keys (serial, UUIDv7) cannot hit this. Consumers should treat rows as
// idempotent upserts by key, as standard CDC practice.
func (c *Coordinator[P, W]) OnStreamedRow(table TableID, pk PrimaryKey) (removed bool) {
	if c.done || c.current == nil || table != *c.current {
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
	if c.done {
		// Start sets done on a resume that finds nothing left, and it has no
		// emit to hand the terminal checkpoint to. Flush it on the first
		// commit instead.
		if c.doneEmitted {
			return false, nil
		}
		if err := c.emitTerminalState(emit); err != nil {
			return false, err
		}
		return true, nil
	}

	if !c.windowOpened && c.low.OpensAt(pos) {
		c.windowOpened = true
	}

	// Both watermarks must be clear of pos: they bracket the chunk read, so
	// the later of the two is the real bound.
	if !c.windowOpened || !c.low.ClosesAt(pos) || !c.high.ClosesAt(pos) {
		return false, nil
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
		if c.done {
			return c.emitTerminalState(emit)
		}
		if !c.readUndisturbed() {
			return nil
		}
		if err := c.releaseWindow(emit); err != nil {
			return err
		}
	}
	if err := c.planNextChunk(ctx); err != nil {
		return err
	}
	if c.done {
		return c.emitTerminalState(emit)
	}
	return nil
}

// emitTerminalState releases the Done checkpoint, once. The window is empty by
// then -- only a zero-row read completes the snapshot -- so this emits state
// alone.
func (c *Coordinator[P, W]) emitTerminalState(emit EmitFunc) error {
	if c.doneEmitted {
		return nil
	}
	c.doneEmitted = true
	if err := c.releaseWindow(emit); err != nil {
		// Nothing was delivered, so the terminal checkpoint is still owed.
		c.doneEmitted = false
		return err
	}
	return nil
}

// readUndisturbed reports whether the buffered chunk's watermarks prove a
// still database: nothing in flight at either end, and nothing assigned
// between them.
func (c *Coordinator[P, W]) readUndisturbed() bool {
	return c.low == c.high && c.low.Quiesced()
}

// State returns the resumable state; safe to call any time after Start. It
// reports the committed fields, except when done -- only a zero-row read can
// finish the snapshot, and that buffers nothing.
func (c *Coordinator[P, W]) State() *State {
	if c.done {
		return &State{Version: CurrentStateVersion, Done: true}
	}
	s := &State{
		Version:         CurrentStateVersion,
		CurrentTable:    c.committedCurrent,
		LastSentPK:      c.committedLastSentPK,
		MaxPK:           c.committedMaxPK,
		RemainingTables: c.committedRemaining,
	}
	return s.Clone()
}

// planNextChunk buffers the current table's next chunk, advancing tables
// until one has rows or none remain.
func (c *Coordinator[P, W]) planNextChunk(ctx context.Context) error {
	for {
		if c.current == nil || c.currentExhausted {
			if len(c.remaining) == 0 {
				c.current = nil
				c.currentExhausted = false
				c.done = true
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
			return err
		}

		if err := c.resolveMaxPK(ctx, table, pkCols); err != nil {
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
