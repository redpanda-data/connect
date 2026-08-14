// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

// Package incrementalsnapshot provides database-agnostic building blocks for
// incremental snapshotting: table/PK identifiers, a dedup window buffer,
// resumable state, and the Deps injection contract a per-database
// coordinator depends on. Every side effect is injected by the caller, so
// this package has no database driver dependency.
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

// PrimaryKey represents a (possibly composite) primary key value, with one
// element per primary key column, in column order.
type PrimaryKey []any

// Row is a single row read from a table during the snapshot phase, along
// with enough metadata for the caller to turn it into a synthetic change
// event.
type Row struct {
	Table TableID
	PK    PrimaryKey
	Data  map[string]any
	// ColumnSchema is opaque to this package. Callers may attach whatever
	// schema/type metadata they need to decode Data downstream.
	ColumnSchema any
}
