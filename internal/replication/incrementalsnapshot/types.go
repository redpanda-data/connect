// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

// Package incrementalsnapshot does incremental snapshots of database tables.
// It reads each table in chunks. The chunks are in primary key order and
// each chunk has a lower and an upper key bound. A replication stream runs
// at the same time. The package removes each buffered row that the stream
// has already delivered.
//
// The package works with any database. Coordinator holds the algorithm. The
// caller supplies all database-specific parts. Deps supplies the operations
// that have side effects. Watermark supplies the comparisons that open and
// close the window. Therefore this package needs no database driver. The
// package also supplies table and primary key identifiers, the window
// buffer and the State checkpoint.
//
// A component for one database supplies a Deps and a Watermark. The
// component usually also declares aliases for Coordinator with its own
// position and watermark types. The aliases keep the type arguments out of
// the call sites.
package incrementalsnapshot

import "fmt"

// TableID identifies a table by schema and name.
type TableID struct {
	Schema string
	Table  string
}

// String returns the "schema.table" representation of the TableID.
func (t TableID) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// PrimaryKey is one primary key value. It has one element for each primary
// key column, in column order. A composite key has more than one element.
type PrimaryKey []any

// Row is one row that the snapshot read from a table. It also holds the
// metadata that the caller needs to make a change event from the row.
type Row struct {
	Table TableID
	PK    PrimaryKey
	Data  map[string]any
	// ColumnSchema has no meaning in this package. The caller can put any
	// schema metadata here that it needs later to decode Data.
	ColumnSchema any
}
