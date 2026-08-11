// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

import (
	"context"
	"fmt"
	"slices"
)

// Coordinator drives Debezium's read-only incremental snapshot algorithm:
// it reads a set of tables in ordered, primary-key-bounded chunks while a
// live replication stream keeps flowing concurrently, deduplicating
// buffered snapshot rows against anything the replication stream has
// already delivered more recently.
//
// Usage contract: Coordinator is NOT safe for concurrent use. Callers must
// invoke OnStreamedRow and OnCommit from a single goroutine, strictly in the
// order the corresponding events occurred on the replication stream.
// OnCommit's chunk-fetch may block on I/O, since the functions in Deps
// (FetchChunk, ResolveWatermark, etc.) run synchronously on the calling
// goroutine. This is intentional -- it mirrors Debezium's own
// single-threaded design -- and should not be "fixed" by introducing
// concurrency here.
type Coordinator struct {
	cfg Config

	// resume holds the state passed to NewCoordinator until Start consumes
	// it; nil once Start has run.
	resume *State

	remaining    []TableID
	current      *TableID
	pkCols       map[string][]string
	maxPK        PrimaryKey
	lastSentPK   PrimaryKey
	low          Watermark
	high         Watermark
	windowOpened bool
	done         bool
	window       *windowBuffer

	// committed mirrors remaining/current/maxPK/lastSentPK, but only ever
	// advances when a chunk is actually flushed (see OnCommit and Start). It
	// deliberately lags behind the live fields above whenever a chunk has
	// been fetched into window but not yet flushed: State() reports
	// committed, never the live fields, so a crash between fetching a chunk
	// and flushing it can never resume past that chunk's rows -- planNextChunk
	// will simply refetch them. Without this split, a resumed coordinator
	// would silently skip any chunk that was fetched but not yet flushed at
	// the moment State() was last captured.
	committedRemaining  []TableID
	committedCurrent    *TableID
	committedMaxPK      PrimaryKey
	committedLastSentPK PrimaryKey
}

// NewCoordinator constructs a Coordinator. If resume is non-nil, the
// coordinator picks up where that state left off once Start is called;
// otherwise it starts fresh from cfg.Tables.
func NewCoordinator(cfg Config, resume *State) (*Coordinator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Coordinator{
		cfg:    cfg,
		resume: resume.Clone(),
		pkCols: make(map[string][]string),
		window: newWindowBuffer(),
	}, nil
}

// Start must be called once before any other method. See the Coordinator
// doc comment and the package-level algorithm description for behavior.
func (c *Coordinator) Start(ctx context.Context) error {
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

	// committed must start in sync with the live fields above: this is the
	// baseline nothing-fetched-yet-this-cycle position (whether fresh or
	// resumed), and planNextChunk below is about to advance the live fields
	// past it to fetch the first not-yet-flushed chunk.
	c.commitLiveState()

	// A persisted State never captures an in-flight, unflushed chunk (the
	// window buffer itself is never persisted), so resuming always means
	// planning the next chunk. Watermarks are re-derived from scratch here
	// via planNextChunk -> ResolveWatermark; they are never read from resume,
	// since State intentionally has no watermark fields.
	return c.planNextChunk(ctx)
}

// commitLiveState snapshots the live remaining/current/maxPK/lastSentPK
// fields into their committed counterparts. Must only be called when
// everything fetched so far has also been flushed -- i.e. right before
// planNextChunk is about to fetch a chunk that hasn't been flushed yet.
func (c *Coordinator) commitLiveState() {
	c.committedRemaining = slices.Clone(c.remaining)
	c.committedCurrent = c.current
	c.committedMaxPK = c.maxPK
	c.committedLastSentPK = c.lastSentPK
}

// Done reports whether every configured table has been fully snapshotted.
func (c *Coordinator) Done() bool {
	return c.done
}

// OnStreamedRow must be cheap and do no I/O. It removes pk from the
// currently buffered window if, and only if, table is the table currently
// being snapshotted -- rows for any other table, or any row once snapshotting
// has completed, are ignored.
//
// Known limitation: this only dedups a row if its streamed event arrives
// while that row is already sitting in the window (i.e. after the chunk
// containing it was fetched, before the window closes). A row inserted with
// a primary key at or below the table's already-resolved MaxPK bound, whose
// streamed INSERT is processed before the chunk that will eventually
// contain it has been fetched, gets no further dedup opportunity once that
// later chunk fetch picks it up -- it will be delivered twice (once live,
// once via backfill). This cannot happen for monotonically increasing
// primary keys (serial/identity columns, UUIDv7, etc): a row inserted after
// a table's scan begins always gets a key above that table's MaxPK bound and
// so is never included in any chunk fetch. It can happen if primary keys are
// reused or backfilled into gaps below the frozen bound while the table is
// being scanned. This mirrors an equivalent limitation in Debezium's own
// incremental snapshot for primary-key-changing updates; consumers should
// treat incremental snapshot rows as idempotent upserts by primary key, as
// is standard CDC practice.
func (c *Coordinator) OnStreamedRow(table TableID, pk PrimaryKey) (removed bool) {
	if c.done || c.current == nil || table != *c.current {
		return false
	}
	return c.window.Remove(table, pk)
}

// OnCommit is called once per completed transaction (regardless of which
// table(s) it touched) with that transaction's txid. txid == 0 is treated as
// "unknown/no-op": some callers may not always have a txid available (e.g.
// empty transactions), and such calls must never open or close the window.
func (c *Coordinator) OnCommit(ctx context.Context, txid uint64) (emitted []Row, changed bool, err error) {
	if c.done || txid == 0 {
		return nil, false, nil
	}

	if !c.windowOpened && txid >= c.low.Xmin {
		c.windowOpened = true
	}

	closeThreshold := max(c.high.Xmax, c.low.Xmax)

	if !c.windowOpened || txid <= closeThreshold {
		return nil, false, nil
	}

	emitted = c.window.Flush()
	c.windowOpened = false

	// Everything fetched so far has now been flushed (emitted, above), so
	// it's safe to advance committed to match live before planNextChunk
	// fetches the next, not-yet-flushed chunk.
	c.commitLiveState()

	if err := c.planNextChunk(ctx); err != nil {
		return nil, false, err
	}

	return emitted, true, nil
}

// State returns a snapshot of the coordinator's current resumable state. It
// is safe to call anytime after Start.
//
// This reports committed*, not the live fields, except when c.done is true:
// done can only become true immediately after a chunk fetch that returned
// zero rows, which never buffers anything into window, so there is never an
// unflushed chunk hiding behind it -- it's always safe to report Done
// immediately, even on a round where committed hasn't caught up to done yet.
func (c *Coordinator) State() *State {
	if c.done {
		return &State{Version: currentStateVersion, Done: true}
	}
	s := &State{
		Version:         currentStateVersion,
		CurrentTable:    c.committedCurrent,
		LastSentPK:      c.committedLastSentPK,
		MaxPK:           c.committedMaxPK,
		RemainingTables: c.committedRemaining,
	}
	return s.Clone()
}

// planNextChunk advances the snapshot by exactly one round of chunk
// fetching: it either buffers the next chunk of the current table, or
// exhausts the current table and moves on to the next one, repeating until
// it has something to buffer or has run out of tables entirely.
func (c *Coordinator) planNextChunk(ctx context.Context) error {
	for {
		if c.current == nil {
			if len(c.remaining) == 0 {
				c.done = true
				return nil
			}

			next := c.remaining[0]
			c.remaining = c.remaining[1:]
			c.current = &next
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

		low, err := c.resolveFreshWatermark(ctx)
		if err != nil {
			return err
		}

		query, args, err := buildChunkQuery(table, pkCols, c.lastSentPK, c.maxPK, c.cfg.ChunkSize)
		if err != nil {
			return fmt.Errorf("building chunk query for table %s: %w", table, err)
		}

		rows, err := c.cfg.Deps.FetchChunk(ctx, table, pkCols, query, args)
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
			// Table is exhausted; advance to the next table and keep
			// looping until we find one with rows, or run out entirely.
			c.current = nil
			continue
		}

		for _, row := range rows {
			c.window.Add(row)
		}
		c.lastSentPK = rows[len(rows)-1].PK

		if len(rows) < c.cfg.ChunkSize {
			// Final, partial chunk for this table. Buffer it now (still
			// emit this chunk through the normal window cycle), but avoid a
			// wasted extra round-trip next time by advancing to the next
			// table up front.
			c.current = nil
		}

		return nil
	}
}

func (c *Coordinator) resolvePKCols(ctx context.Context, table TableID) ([]string, error) {
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

func (c *Coordinator) resolveMaxPK(ctx context.Context, table TableID, pkCols []string) error {
	if c.maxPK != nil {
		return nil
	}

	query, err := buildMaxKeyQuery(table, pkCols)
	if err != nil {
		return fmt.Errorf("building max key query for table %s: %w", table, err)
	}

	maxPK, err := c.cfg.Deps.ResolveMaxKey(ctx, table, pkCols, query)
	if err != nil {
		return fmt.Errorf("resolving max key for table %s: %w", table, err)
	}
	c.maxPK = maxPK
	return nil
}

// resolveFreshWatermark forces a fresh transaction before resolving the
// watermark, since a long-lived connection may otherwise observe a stale
// snapshot (e.g. under REPEATABLE READ isolation).
func (c *Coordinator) resolveFreshWatermark(ctx context.Context) (Watermark, error) {
	if err := c.cfg.Deps.ForceFreshTransaction(ctx); err != nil {
		return Watermark{}, fmt.Errorf("forcing fresh transaction: %w", err)
	}
	wm, err := c.cfg.Deps.ResolveWatermark(ctx)
	if err != nil {
		return Watermark{}, fmt.Errorf("resolving watermark: %w", err)
	}
	return wm, nil
}
