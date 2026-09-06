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

// Deps supplies all operations that have side effects to the coordinator.
// A component for one database implements it. Therefore the coordinator
// needs no database driver.
//
// The coordinator selects the next table and the next primary key range.
// The implementation then makes its own query for that range. This package
// builds no SQL, because key range pagination is not the same on all
// databases. Postgres and MySQL compare row constructors, but Oracle cannot
// do this.
//
// W is the watermark type. It must satisfy Watermark[P] for the position
// type of the coordinator. The coordinator calls all methods from one
// goroutine. A method can block on I/O.
type Deps[W any] interface {
	// ResolvePrimaryKey returns the primary key columns of the table. The
	// names are not quoted. The coordinator keeps the result for each table
	// in a cache.
	ResolvePrimaryKey(ctx context.Context, table TableID) (columns []string, err error)

	// ResolveMaxKey returns the largest primary key in the table now. This
	// key is the upper bound of the snapshot. The replication stream then
	// delivers all rows that come after the snapshot starts.
	//
	// A nil PrimaryKey with a nil error shows that the table has no rows.
	// This result is not an error.
	ResolveMaxKey(ctx context.Context, table TableID, pkColumnsUnquoted []string) (PrimaryKey, error)

	// ResolveWatermark reads a new watermark.
	ResolveWatermark(ctx context.Context) (W, error)

	// ForceFreshTransaction gives the connection a real transaction id. For
	// example, it can start a small transaction and commit it. The
	// replication stream then has a known position for the first watermark.
	//
	// Start calls this method one time only. Do not call it for each chunk.
	// A call for each watermark uses two ids for each chunk. It also makes
	// the two watermarks of each read different, and this stops the drain.
	ForceFreshTransaction(ctx context.Context) error

	// FetchChunk returns a maximum of limit rows from the table, in primary
	// key order. The rows have a key that is larger than lower and smaller
	// than upper or equal to it. A nil lower means that the chunk is the
	// first chunk of the table and has no lower bound. Upper is never nil.
	//
	// Fewer rows than limit tell the coordinator that the table has no more
	// rows.
	FetchChunk(ctx context.Context, table TableID, pkColumnsUnquoted []string, lower, upper PrimaryKey, limit int) ([]Row, error)
}

// CoordinatorConfig holds the configuration of a coordinator. P is the
// transaction position type of the database. W is its watermark type. Refer
// to Watermark.
type CoordinatorConfig[P any, W Watermark[P]] struct {
	Tables    []TableID
	ChunkSize int
	Deps      Deps[W]
	// MaxDrainChunks is the maximum number of chunks that one OnCommit can
	// release. OnCommit releases more than one chunk only when the database
	// is quiet and needs no row removal.
	//
	// The coordinator emits on the replication loop of the caller. This
	// limit controls how long it holds that loop. The loop is then free for
	// other work, such as standby keepalive messages.
	//
	// A zero value selects DefaultMaxDrainChunks. A negative value stops the
	// drain, and the snapshot then releases one chunk for each commit.
	MaxDrainChunks int
}

// DefaultMaxDrainChunks is the drain limit that the coordinator uses when
// CoordinatorConfig.MaxDrainChunks is zero.
const DefaultMaxDrainChunks = 32

// Validate makes sure that the configuration is usable. It returns a clear
// error here instead of an unclear failure later in the algorithm.
func (c CoordinatorConfig[P, W]) Validate() error {
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be > 0, got %d", c.ChunkSize)
	}
	if c.Deps == nil {
		return errors.New("incrementalsnapshot: Deps must not be nil")
	}
	return nil
}
