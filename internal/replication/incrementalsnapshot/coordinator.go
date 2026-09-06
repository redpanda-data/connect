// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"context"
	"fmt"
	"slices"
)

// Coordinator backfills a set of tables in ordered, PK-bounded chunks
// while a live replication stream flows concurrently, deduplicating
// buffered rows against anything the stream already delivered.
//
// It is database-agnostic: every side effect comes from Deps, and the
// open/close reconciliation is delegated to the Watermark implementation, so
// the only thing here is the algorithm -- which table and key range to read
// next, when a buffered chunk is safe to emit, and what may be checkpointed.
//
// Not safe for concurrent use: OnStreamedRow and OnCommit must be called
// from a single goroutine, in stream order. OnCommit's chunk fetch may
// block on I/O -- intentional, not a bug to fix with concurrency.
type Coordinator[P any, W Watermark[P]] struct {
	cfg CoordinatorConfig[P, W]

	// resume holds the state passed to NewCoordinator until Start consumes
	// it; nil once Start has run.
	resume *State

	remaining []TableID
	current   *TableID
	// currentExhausted marks current's last chunk as already fetched, so
	// the next plan advances to the next table. current stays set until
	// then so OnStreamedRow can keep deduping the buffered final chunk.
	// Deliberately not part of State: on resume the coordinator re-issues
	// one empty chunk query for the table and advances from there.
	currentExhausted bool
	pkCols           map[string][]string
	maxPK            PrimaryKey
	lastSentPK       PrimaryKey
	low              W
	high             W
	windowOpened     bool
	done             bool
	window           *WindowBuffer

	// committed mirrors remaining/current/maxPK/lastSentPK, but only
	// advances once a chunk is flushed. State() reports committed, not
	// live, so a crash before a flush re-fetches the chunk on resume
	// instead of skipping it.
	committedRemaining  []TableID
	committedCurrent    *TableID
	committedMaxPK      PrimaryKey
	committedLastSentPK PrimaryKey
}

// NewCoordinator constructs a Coordinator. If resume is non-nil, the
// coordinator picks up where that state left off once Start is called;
// otherwise it starts fresh from cfg.Tables.
func NewCoordinator[P any, W Watermark[P]](cfg CoordinatorConfig[P, W], resume *State) (*Coordinator[P, W], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Coordinator[P, W]{
		cfg:    cfg,
		resume: resume.Clone(),
		pkCols: make(map[string][]string),
		window: NewWindowBuffer(),
	}, nil
}

// Start must be called once before any other method. See the Coordinator
// doc comment and the package-level algorithm description for behavior.
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

	// State never captures an unflushed chunk, so resuming always means
	// planning the next one. Watermarks are always re-derived, never
	// read from resume.
	return c.planNextChunk(ctx)
}

// commitLiveState snapshots the live fields into their committed
// counterparts. Call only once everything fetched so far has been flushed.
func (c *Coordinator[P, W]) commitLiveState() {
	c.committedRemaining = slices.Clone(c.remaining)
	c.committedCurrent = c.current
	c.committedMaxPK = c.maxPK
	c.committedLastSentPK = c.lastSentPK
}

// Done reports whether every configured table has been fully snapshotted.
func (c *Coordinator[P, W]) Done() bool {
	return c.done
}

// OnStreamedRow must be cheap and do no I/O. It removes pk from the
// buffered window only if table is currently being snapshotted;
// otherwise, or once done, it's a no-op.
//
// Known limitation: a reused/backfilled PK below MaxPK can be delivered
// twice if its INSERT streams in before the covering chunk is fetched
// (monotonic keys like serial/UUIDv7 can't hit this). Consumers should
// treat rows as idempotent upserts by PK, as standard CDC practice.
func (c *Coordinator[P, W]) OnStreamedRow(table TableID, pk PrimaryKey) (removed bool) {
	if c.done || c.current == nil || table != *c.current {
		return false
	}
	return c.window.Remove(table, pk)
}

// OnCommit is called once per completed transaction with the position it
// committed at, in stream order.
//
// Callers must only pass a position the database actually reported. A
// synthetic or unknown position (a zero value standing in for "no BEGIN
// seen", say) can open or close the window spuriously and must be filtered
// out before calling.
func (c *Coordinator[P, W]) OnCommit(ctx context.Context, pos P) (emitted []Row, changed bool, err error) {
	if c.done {
		return nil, false, nil
	}

	if !c.windowOpened && c.low.OpensAt(pos) {
		c.windowOpened = true
	}

	// Both watermarks must be clear of pos: the pair brackets the chunk
	// read, so the later of the two is what actually bounds it.
	if !c.windowOpened || !c.low.ClosesAt(pos) || !c.high.ClosesAt(pos) {
		return nil, false, nil
	}

	emitted = c.window.Flush()
	c.windowOpened = false

	// Flushed, so it's now safe to commit before fetching the next chunk.
	c.commitLiveState()

	if err := c.planNextChunk(ctx); err != nil {
		return nil, false, err
	}

	return emitted, true, nil
}

// State returns the coordinator's resumable state; safe to call anytime
// after Start. Reports committed*, not live, except when done -- a
// zero-row fetch (the only way done becomes true) never buffers
// anything, so there's nothing unflushed to hide.
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
			// Empty table -- treat like an exhausted chunk rather than
			// failing the whole coordinator.
			c.current = nil
			continue
		}

		low, err := c.resolveFreshWatermark(ctx)
		if err != nil {
			return err
		}

		rows, err := c.cfg.Deps.FetchChunk(ctx, table, pkCols, c.lastSentPK, c.maxPK, c.cfg.ChunkSize)
		if err != nil {
			return fmt.Errorf("fetching chunk for table %s: %w", table, err)
		}

		high, err := c.resolveFreshWatermark(ctx)
		if err != nil {
			return err
		}

		c.low = low
		c.high = high

		if len(rows) == 0 {
			// Exhausted; keep looping until a table has rows or none remain.
			c.current = nil
			continue
		}

		for _, row := range rows {
			c.window.Add(row)
		}
		c.lastSentPK = rows[len(rows)-1].PK

		if len(rows) < c.cfg.ChunkSize {
			// Final, partial chunk. Mark the table exhausted so the next
			// plan advances past it (skipping a wasted empty round-trip),
			// but leave c.current set: these rows are still buffered in
			// the window, and OnStreamedRow can only dedup against them
			// while current names their table. Clearing it here would let
			// a concurrent UPDATE/DELETE stream past undeduped and be
			// overwritten by this stale chunk when the window flushes.
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

// resolveMaxPK resolves and caches the table's max PK. Leaves c.maxPK nil
// if the table has no rows; planNextChunk treats that as nothing to
// backfill.
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

// resolveFreshWatermark forces a fresh transaction first, since a long-lived
// connection could otherwise see a stale snapshot.
func (c *Coordinator[P, W]) resolveFreshWatermark(ctx context.Context) (W, error) {
	var zero W
	if err := c.cfg.Deps.ForceFreshTransaction(ctx); err != nil {
		return zero, fmt.Errorf("forcing fresh transaction: %w", err)
	}
	wm, err := c.cfg.Deps.ResolveWatermark(ctx)
	if err != nil {
		return zero, fmt.Errorf("resolving watermark: %w", err)
	}
	return wm, nil
}
