// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

// Package snapshot implements Debezium's read-only incremental snapshot
// algorithm for Postgres, decoupled from any concrete driver. The package is
// pure algorithm: every side effect (querying Postgres, resolving primary
// keys, etc.) is injected via the Deps struct in deps.go, so this package has
// zero dependency on pgx, database/sql, or the pglogicalstream package. This
// keeps the dependency graph acyclic: pglogicalstream imports snapshot, never
// the reverse.
package snapshot

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
