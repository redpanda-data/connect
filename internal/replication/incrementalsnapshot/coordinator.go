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

// Coordinator reads a set of tables in chunks while a replication stream
// runs at the same time. The chunks are in primary key order and each chunk
// has a lower and an upper key bound. The Coordinator removes each buffered
// row that the stream has already delivered.
//
// The Coordinator works with any database. All side effects come from Deps.
// The Watermark implementation does the comparisons that open and close the
// window. Therefore this type holds the algorithm only. The algorithm
// selects the next table and key range, decides when a chunk is safe to
// emit, and reports what the caller can checkpoint.
//
// The Coordinator is not safe for concurrent use. Call OnStreamedRow and
// OnCommit from one goroutine only, in stream order. The chunk read in
// OnCommit can block on I/O. This behaviour is intentional.
type Coordinator[P any, W Watermark[P]] struct {
	cfg CoordinatorConfig[P, W]

	// resume holds the state that the caller gave to NewCoordinator. Start
	// uses it and then sets it to nil.
	resume *State

	remaining []TableID
	current   *TableID
	// currentExhausted shows that the coordinator has read the last chunk
	// of current. The next plan then moves to the next table. current stays
	// set until then, and OnStreamedRow can therefore still remove rows of
	// the last chunk from the buffer.
	//
	// This field is not part of State. After a resume the coordinator makes
	// one more empty query for the table and then moves on.
	currentExhausted bool
	pkCols           map[string][]string
	maxPK            PrimaryKey
	lastSentPK       PrimaryKey
	low              W
	high             W
	windowOpened     bool
	done             bool
	window           *WindowBuffer

	// The committed fields copy remaining, current, maxPK and lastSentPK.
	// They move forward only after a flush. State returns the committed
	// fields and not the live fields. Therefore a failure before a flush
	// makes the resume read the chunk again instead of skipping it.
	committedRemaining  []TableID
	committedCurrent    *TableID
	committedMaxPK      PrimaryKey
	committedLastSentPK PrimaryKey
}

// NewCoordinator makes a Coordinator. If resume is not nil, Start continues
// the snapshot from that state. If resume is nil, Start begins a new
// snapshot of cfg.Tables.
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

// EmitFunc gets the rows of one chunk when the coordinator releases them.
// The coordinator calls it after it moves the committed state forward past
// those rows. Therefore the function can call State to get the checkpoint
// for the rows.
//
// EmitFunc also controls the speed of the drain. The coordinator releases
// the next chunk only after EmitFunc returns. Therefore a slow consumer
// holds a maximum of one chunk in memory. An error stops the operation.
type EmitFunc func(rows []Row) error

// Start must run one time before all other methods. Refer to Coordinator
// and to the package documentation for the behaviour.
//
// Start emits no rows. It puts the first chunk in the buffer and returns.
// OnCommit then releases all rows. Therefore the caller can call Start
// before the downstream consumer is ready.
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

	// Give the connection a real transaction id one time. The stream then
	// has a known position for the first watermark. Do not repeat this for
	// each watermark. Refer to Deps.ForceFreshTransaction.
	if err := c.cfg.Deps.ForceFreshTransaction(ctx); err != nil {
		return fmt.Errorf("forcing fresh transaction: %w", err)
	}

	// State never holds a chunk that the coordinator has not flushed.
	// Therefore a resume always reads the next chunk. The coordinator always
	// reads new watermarks and never takes them from resume.
	return c.planNextChunk(ctx)
}

// commitLiveState copies the live fields to the committed fields. Call it
// only after the coordinator has flushed all rows that it has read.
func (c *Coordinator[P, W]) commitLiveState() {
	c.committedRemaining = slices.Clone(c.remaining)
	c.committedCurrent = c.current
	c.committedMaxPK = c.maxPK
	c.committedLastSentPK = c.lastSentPK
}

// Done tells if the snapshot of all configured tables is complete.
func (c *Coordinator[P, W]) Done() bool {
	return c.done
}

// OnStreamedRow must be fast and must do no I/O. It removes pk from the
// buffer only while the snapshot reads that table. In all other cases, and
// after the snapshot is complete, it does nothing.
//
// Known limit: the snapshot can deliver a row two times. This happens when
// an insert uses a primary key that is smaller than MaxPK and the stream
// delivers that insert before the chunk that holds the key. Keys that
// always increase, such as serial or UUIDv7 keys, cannot cause this.
// Consumers must write all rows as upserts on the primary key. This
// practice is normal for CDC.
func (c *Coordinator[P, W]) OnStreamedRow(table TableID, pk PrimaryKey) (removed bool) {
	if c.done || c.current == nil || table != *c.current {
		return false
	}
	return c.window.Remove(table, pk)
}

// OnCommit runs one time for each completed transaction. The caller gives
// the position of the commit, in stream order.
//
// Give only a position that the database reported. Remove all unknown
// positions before the call. For example, a zero value that shows a missing
// BEGIN message can open or close the window at the wrong time.
func (c *Coordinator[P, W]) OnCommit(ctx context.Context, pos P, emit EmitFunc) (changed bool, err error) {
	if c.done {
		return false, nil
	}

	if !c.windowOpened && c.low.OpensAt(pos) {
		c.windowOpened = true
	}

	// Pos must come after both watermarks. The two watermarks are on each
	// side of the chunk read, so the later watermark is the true bound.
	if !c.windowOpened || !c.low.ClosesAt(pos) || !c.high.ClosesAt(pos) {
		return false, nil
	}

	if err := c.releaseWindow(emit); err != nil {
		return false, err
	}
	return true, c.planAndDrain(ctx, emit)
}

// releaseWindow gives the buffered chunk to emit. It first moves the
// committed state forward past the chunk. State then returns the correct
// checkpoint when emit calls it.
func (c *Coordinator[P, W]) releaseWindow(emit EmitFunc) error {
	rows := c.window.Flush()
	c.windowOpened = false
	c.commitLiveState()
	return emit(rows)
}

// planAndDrain puts the next chunk in the buffer. It then releases that
// chunk and reads another one while each chunk comes from an undisturbed
// read.
//
// Two equal and quiesced watermarks show that the database was quiet during
// the read. No transaction could change the chunk, so no streamed row can
// replace a row in it. Therefore the coordinator can release the chunk now.
// If it keeps the chunk, it must wait for the next commit. On a quiet table
// that commit comes only with the next heartbeat. This wait is what limits
// the snapshot to one chunk for each heartbeat.
//
// Each call releases a maximum of cfg.MaxDrainChunks chunks. The coordinator
// emits on the replication loop of the caller, and that loop usually also
// sends standby keepalive messages. Without this limit, the server can end
// the connection. The snapshot continues at the next commit.
func (c *Coordinator[P, W]) planAndDrain(ctx context.Context, emit EmitFunc) error {
	for range c.cfg.MaxDrainChunks {
		if err := c.planNextChunk(ctx); err != nil {
			return err
		}
		if c.done || !c.readUndisturbed() {
			return nil
		}
		if err := c.releaseWindow(emit); err != nil {
			return err
		}
	}
	return c.planNextChunk(ctx)
}

// readUndisturbed tells if the two watermarks of the buffered chunk show a
// quiet database. A quiet database has no transaction in flight at either
// watermark and starts no transaction between them.
func (c *Coordinator[P, W]) readUndisturbed() bool {
	return c.low == c.high && c.low.Quiesced()
}

// State returns the state that a later run can resume from. The caller can
// call it at any time after Start. It returns the committed fields and not
// the live fields.
//
// When the snapshot is complete, the live fields are safe to report. Only a
// read of zero rows can complete the snapshot, and such a read puts nothing
// in the buffer.
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

// planNextChunk puts the next chunk of the current table in the buffer. It
// moves to the next table until it finds rows or no table remains.
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
			// The table is empty. Continue as for a table with no more
			// rows. Do not stop the coordinator.
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
			// The table has no more rows. Continue until a table has rows
			// or no table remains.
			c.current = nil
			continue
		}

		for _, row := range rows {
			c.window.Add(row)
		}
		c.lastSentPK = rows[len(rows)-1].PK

		if len(rows) < c.cfg.ChunkSize {
			// This chunk is the last chunk of the table and is not full.
			// Mark the table complete, and the next plan then moves to
			// the next table. This saves one empty query.
			//
			// Keep c.current set. The rows of this chunk are still in the
			// buffer, and OnStreamedRow can remove them only while
			// c.current holds their table. If c.current were nil here, an
			// update or a delete could stream past the buffer. The old
			// chunk would then replace the new row at the flush.
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

// resolveMaxPK reads the largest primary key of the table and keeps it in a
// cache. It leaves c.maxPK nil if the table has no rows. planNextChunk then
// reads no rows from that table.
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

// resolveWatermark reads one watermark. It does not force a new transaction
// first. Each read is its own statement and therefore already sees a
// current snapshot. A new transaction id here would also make the two
// watermarks of each chunk different, and readUndisturbed needs them to be
// equal.
func (c *Coordinator[P, W]) resolveWatermark(ctx context.Context) (W, error) {
	var zero W
	wm, err := c.cfg.Deps.ResolveWatermark(ctx)
	if err != nil {
		return zero, fmt.Errorf("resolving watermark: %w", err)
	}
	return wm, nil
}
