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
	"errors"
	"fmt"
)

// Deps is implemented by each database-specific component to supply every
// side-effecting operation an incremental snapshot coordinator needs,
// keeping the coordinator itself free of any concrete database driver
// dependency.
//
// The coordinator decides which table and which primary key range to read
// next; how that range becomes a query is entirely the implementation's
// business. Keyset pagination over a composite primary key is not portable
// (Postgres and MySQL compare row constructors, Oracle cannot), so no SQL is
// built here.
//
// W is the concrete watermark type, which must satisfy Watermark[P] for the
// coordinator's position type. Methods are called from the coordinator's
// single goroutine and may block on I/O.
type Deps[W any] interface {
	// ResolvePrimaryKey returns the primary key columns (unquoted) for the
	// given table. The coordinator caches the result per table.
	ResolvePrimaryKey(ctx context.Context, table TableID) (columns []string, err error)

	// ResolveMaxKey returns the table's current maximum primary key, which
	// fixes the upper bound of the backfill so rows inserted after the
	// snapshot starts are left to the replication stream. A nil PrimaryKey
	// with a nil error means the table currently has no rows -- there is
	// nothing to backfill, as opposed to a real error.
	ResolveMaxKey(ctx context.Context, table TableID, pkColumnsUnquoted []string) (PrimaryKey, error)

	// ResolveWatermark returns a fresh watermark.
	ResolveWatermark(ctx context.Context) (W, error)

	// ForceFreshTransaction ensures the next watermark resolution observes
	// a fresh transaction snapshot (e.g. by starting and committing a
	// trivial transaction).
	ForceFreshTransaction(ctx context.Context) error

	// FetchChunk returns up to limit rows of table ordered by primary key,
	// covering the keys after lower up to and including upper. A nil lower
	// means unbounded below (the table's first chunk); upper is never nil.
	// Returning fewer than limit rows tells the coordinator the table is
	// exhausted.
	FetchChunk(ctx context.Context, table TableID, pkColumnsUnquoted []string, lower, upper PrimaryKey, limit int) ([]Row, error)
}

// CoordinatorConfig configures an incremental snapshot coordinator. P is the
// database's transaction position type and W its concrete watermark type;
// see Watermark.
type CoordinatorConfig[P any, W Watermark[P]] struct {
	Tables    []TableID
	ChunkSize int
	Deps      Deps[W]
}

// Validate checks that the config is usable, returning a clear error rather
// than failing confusingly deep inside the algorithm.
func (c CoordinatorConfig[P, W]) Validate() error {
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be > 0, got %d", c.ChunkSize)
	}
	if c.Deps == nil {
		return errors.New("incrementalsnapshot: Deps must not be nil")
	}
	return nil
}
