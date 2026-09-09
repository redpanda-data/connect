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
)

// Deps supplies every side-effecting operation the coordinator needs, so the
// coordinator itself carries no database driver.
//
// The coordinator picks the next table and key range; the implementation
// builds its own query for it. This package builds no SQL, because keyset
// pagination is not portable -- Postgres and MySQL compare row constructors,
// Oracle cannot.
//
// W must satisfy Watermark[P]. Methods are called from a single goroutine
// and may block on I/O.
type Deps[W any] interface {
	// ResolvePrimaryKey returns the table's unquoted key columns. The
	// coordinator caches the result per table.
	ResolvePrimaryKey(ctx context.Context, table TableID) (columns []string, err error)

	// ResolveMaxKey returns the table's current largest key, which bounds the
	// backfill: rows inserted after that are left to the stream. A nil key
	// with a nil error means the table is empty, which is not an error.
	ResolveMaxKey(ctx context.Context, table TableID, pkColumnsUnquoted []string) (PrimaryKey, error)

	// ResolveWatermark reads a fresh watermark.
	ResolveWatermark(ctx context.Context) (W, error)

	// ForceFreshTransaction assigns the connection a real transaction id, so
	// the stream has a known position for the first watermark.
	//
	// Start calls it once. Calling it per watermark would make every chunk's
	// two watermarks differ, which disables the drain entirely.
	ForceFreshTransaction(ctx context.Context) error

	// FetchChunk returns up to limit rows in key order, covering the keys
	// after lower up to and including upper. A nil lower means the table's
	// first chunk; upper is never nil. Returning fewer than limit rows tells
	// the coordinator the table is exhausted.
	FetchChunk(ctx context.Context, table TableID, pkColumnsUnquoted []string, lower, upper PrimaryKey, limit int) ([]Row, error)
}

// CoordinatorConfig configures a Coordinator. P is the database's position
// type and W its watermark type; see Watermark.
type CoordinatorConfig[P any, W Watermark[P]] struct {
	Tables    []TableID
	ChunkSize int
	Deps      Deps[W]
	// MaxDrainChunks caps how many chunks one OnCommit may release, which it
	// only does while the database is quiet enough to need no deduplication.
	// Emitting runs on the caller's replication loop, so this bounds how long
	// that loop is held -- leaving it free for standby keepalives and such.
	//
	// Zero selects DefaultMaxDrainChunks; a negative value disables draining.
	MaxDrainChunks int
}

// DefaultMaxDrainChunks applies when CoordinatorConfig.MaxDrainChunks is
// zero.
const DefaultMaxDrainChunks = 32

// Validate checks the config is usable, so a mistake surfaces here rather
// than deep inside the algorithm.
func (c CoordinatorConfig[P, W]) Validate() error {
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be > 0, got %d", c.ChunkSize)
	}
	if c.Deps == nil {
		return errors.New("incrementalsnapshot: Deps must not be nil")
	}
	return nil
}
