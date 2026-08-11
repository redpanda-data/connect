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
)

// Deps injects every side-effecting operation the Coordinator needs, so
// this package stays free of any concrete database driver dependency.
type Deps struct {
	// ResolvePrimaryKey returns the primary key columns (unquoted) for the
	// given table.
	ResolvePrimaryKey func(ctx context.Context, table TableID) (columns []string, err error)
	// ResolveMaxKey executes the query produced by buildMaxKeyQuery and
	// returns the table's current maximum primary key.
	ResolveMaxKey func(ctx context.Context, table TableID, pkColumnsUnquoted []string, query string) (PrimaryKey, error)
	// ResolveWatermark returns a fresh watermark (e.g. by parsing the
	// result of Postgres's txid_current_snapshot()).
	ResolveWatermark func(ctx context.Context) (Watermark, error)
	// ForceFreshTransaction ensures the next watermark resolution observes
	// a fresh transaction snapshot (e.g. by starting and committing a
	// trivial transaction).
	ForceFreshTransaction func(ctx context.Context) error
	// FetchChunk executes the query produced by buildChunkQuery and returns
	// the decoded rows.
	FetchChunk func(ctx context.Context, table TableID, pkColumnsUnquoted []string, query string, args []any) ([]Row, error)
}

// validate checks that the Config is usable, returning a clear error naming
// the specific missing/invalid field. A nil Deps function would otherwise
// panic confusingly deep inside the algorithm rather than failing fast at
// construction time.
func (c Config) validate() error {
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be > 0, got %d", c.ChunkSize)
	}

	type namedFunc struct {
		name string
		set  bool
	}
	fields := []namedFunc{
		{"ResolvePrimaryKey", c.Deps.ResolvePrimaryKey != nil},
		{"ResolveMaxKey", c.Deps.ResolveMaxKey != nil},
		{"ResolveWatermark", c.Deps.ResolveWatermark != nil},
		{"ForceFreshTransaction", c.Deps.ForceFreshTransaction != nil},
		{"FetchChunk", c.Deps.FetchChunk != nil},
	}
	var missing []string
	for _, f := range fields {
		if !f.set {
			missing = append(missing, f.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("snapshot: missing required Deps function(s): %v", missing)
	}

	return nil
}

// Config configures a Coordinator.
type Config struct {
	Tables    []TableID
	ChunkSize int
	Deps      Deps
}
