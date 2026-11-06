// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package incrementalsnapshot backfills database tables in key-ordered,
// key-bounded chunks while a replication stream runs, dropping each buffered
// row the stream has already delivered.
//
// Coordinator holds the algorithm. Everything database-specific is injected:
// side effects through Deps, window comparisons through Watermark. The
// package therefore needs no database driver.
//
// A component for one database supplies a Deps and a Watermark, and usually
// aliases Coordinator to its own position and watermark types to keep the
// type arguments out of its call sites.
//
// Connectors whose stream has no single global log position (for example a
// stream split into shards that each carry their own position, such as
// DynamoDB Streams) cannot bound a chunk with watermarks. They use PageWindow
// and ShardProgress instead. PageWindow holds each scanned page and drops
// every key the stream touches before the page is sent, under one lock, so a
// snapshot item is never emitted after its key's stream event. ShardProgress
// tracks how far each shard has been read and decides when a held page may be
// released; it only refines ordering, never safety. TableQueue runs such
// backfills one table at a time.
package incrementalsnapshot

import "fmt"

// TableID identifies a table by schema and name.
type TableID struct {
	Schema string
	Table  string
}

// String returns the TableID as "schema.table".
func (t TableID) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// PrimaryKey is one key value, with an element per key column in column
// order. A composite key has more than one element.
type PrimaryKey []any

// Row is one row the snapshot read, with the metadata the caller needs to
// turn it into a change event.
type Row struct {
	Table TableID
	PK    PrimaryKey
	Data  map[string]any
	// ColumnSchema is opaque here. Callers may attach whatever schema
	// metadata they need to decode Data later.
	ColumnSchema any
}
