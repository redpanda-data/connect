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

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// Coordinator reads a set of tables in ordered, primary-key-bounded chunks
// while a live replication stream keeps flowing concurrently, deduplicating
// buffered rows against anything the stream has already delivered more
// recently.
//
// Not safe for concurrent use: OnStreamedRow and OnCommit must be called
// from a single goroutine, in the order events occurred on the stream.
// OnCommit's chunk-fetch may block on I/O (Deps functions run synchronously
// on the calling goroutine) -- this single-threaded design is intentional,
// not something to "fix" with concurrency.
type Coordinator struct {
	cfg incrementalsnapshot.Config

	// resume holds the state passed to NewCoordinator until Start consumes
	// it; nil once Start has run.
	resume *incrementalsnapshot.State

	remaining    []incrementalsnapshot.TableID
	current      *incrementalsnapshot.TableID
	pkCols       map[string][]string
	maxPK        incrementalsnapshot.PrimaryKey
	lastSentPK   incrementalsnapshot.PrimaryKey
	low          Watermark
	high         Watermark
	windowOpened bool
	done         bool
	window       *incrementalsnapshot.WindowBuffer

	// committed mirrors remaining/current/maxPK/lastSentPK but only advances
	// once a chunk is actually flushed (see OnCommit/Start). It lags behind
	// the live fields while a chunk sits fetched-but-unflushed: State()
	// reports only committed, so a crash before a flush just re-fetches that
	// chunk on resume instead of silently skipping it.
	committedRemaining  []incrementalsnapshot.TableID
	committedCurrent    *incrementalsnapshot.TableID
	committedMaxPK      incrementalsnapshot.PrimaryKey
	committedLastSentPK incrementalsnapshot.PrimaryKey
}

// NewCoordinator constructs a Coordinator. If resume is non-nil, the
// coordinator picks up where that state left off once Start is called;
// otherwise it starts fresh from cfg.Tables.
func NewCoordinator(cfg incrementalsnapshot.Config, resume *incrementalsnapshot.State) (*Coordinator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Coordinator{
		cfg:    cfg,
		resume: resume.Clone(),
		pkCols: make(map[string][]string),
		window: incrementalsnapshot.NewWindowBuffer(),
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

	// Baseline: nothing fetched yet this cycle. planNextChunk below advances
	// the live fields past it to fetch the first unflushed chunk.
	c.commitLiveState()

	// A persisted State never captures an unflushed chunk (window isn't
	// persisted), so resuming always means planning the next one.
	// Watermarks are always re-derived here, never read from resume.
	return c.planNextChunk(ctx)
}

// commitLiveState snapshots the live fields into their committed
// counterparts. Call only once everything fetched so far has been flushed.
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

// OnStreamedRow must be cheap and do no I/O. It removes pk from the buffered
// window only if table is the one currently being snapshotted; otherwise,
// or once snapshotting is done, it's a no-op.
//
// Known limitation: dedup only works if the row is already in the window
// when its streamed event arrives. A row whose primary key is inserted at
// or below the table's resolved MaxPK bound, and whose INSERT streams in
// before the chunk covering it is fetched, gets delivered twice (live and
// via backfill). This can't happen for monotonically increasing keys
// (serial/identity, UUIDv7) since new rows always land above MaxPK -- only
// reused or backfilled keys below the frozen bound are affected. Consumers
// should treat incremental snapshot rows as idempotent upserts by primary
// key, as is standard CDC practice.
func (c *Coordinator) OnStreamedRow(table incrementalsnapshot.TableID, pk incrementalsnapshot.PrimaryKey) (removed bool) {
	if c.done || c.current == nil || table != *c.current {
		return false
	}
	return c.window.Remove(table, pk)
}

// OnCommit is called once per completed transaction with its txid. txid == 0
// means "unknown" (some callers can't always supply one) and must never
// open or close the window.
func (c *Coordinator) OnCommit(ctx context.Context, txid uint64) (emitted []incrementalsnapshot.Row, changed bool, err error) {
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

	// Everything fetched so far is now flushed, so it's safe to advance
	// committed before planNextChunk fetches the next chunk.
	c.commitLiveState()

	if err := c.planNextChunk(ctx); err != nil {
		return nil, false, err
	}

	return emitted, true, nil
}

// State returns the coordinator's current resumable state; safe to call
// anytime after Start. It reports committed*, not the live fields, except
// when done: a zero-row chunk fetch (the only way done becomes true) never
// buffers anything, so there's no unflushed chunk to hide.
func (c *Coordinator) State() *incrementalsnapshot.State {
	if c.done {
		return &incrementalsnapshot.State{Version: incrementalsnapshot.CurrentStateVersion, Done: true}
	}
	s := &incrementalsnapshot.State{
		Version:         incrementalsnapshot.CurrentStateVersion,
		CurrentTable:    c.committedCurrent,
		LastSentPK:      c.committedLastSentPK,
		MaxPK:           c.committedMaxPK,
		RemainingTables: c.committedRemaining,
	}
	return s.Clone()
}

// planNextChunk advances by one round: buffers the current table's next
// chunk, or exhausts it and moves to the next, until it has rows to buffer
// or runs out of tables.
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
			// Exhausted; keep looping until a table has rows or none remain.
			c.current = nil
			continue
		}

		for _, row := range rows {
			c.window.Add(row)
		}
		c.lastSentPK = rows[len(rows)-1].PK

		if len(rows) < c.cfg.ChunkSize {
			// Final, partial chunk: buffer it as usual, but advance to the
			// next table now to skip a wasted empty round-trip later.
			c.current = nil
		}

		return nil
	}
}

func (c *Coordinator) resolvePKCols(ctx context.Context, table incrementalsnapshot.TableID) ([]string, error) {
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

func (c *Coordinator) resolveMaxPK(ctx context.Context, table incrementalsnapshot.TableID, pkCols []string) error {
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

// resolveFreshWatermark forces a fresh transaction first, since a
// long-lived connection could otherwise see a stale snapshot. Deps.
// ResolveWatermark returns an opaque any, so this is also where it's
// asserted back to the concrete Postgres Watermark.
func (c *Coordinator) resolveFreshWatermark(ctx context.Context) (Watermark, error) {
	if err := c.cfg.Deps.ForceFreshTransaction(ctx); err != nil {
		return Watermark{}, fmt.Errorf("forcing fresh transaction: %w", err)
	}
	wmAny, err := c.cfg.Deps.ResolveWatermark(ctx)
	if err != nil {
		return Watermark{}, fmt.Errorf("resolving watermark: %w", err)
	}
	wm, ok := wmAny.(Watermark)
	if !ok {
		return Watermark{}, fmt.Errorf("resolving watermark: Deps.ResolveWatermark returned %T, expected snapshot.Watermark", wmAny)
	}
	return wm, nil
}
