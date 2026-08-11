// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

// Watermark is a point-in-time view of which transactions a database
// considered in flight. The coordinator takes one either side of every chunk
// read and reconciles the pair against the positions of transactions the
// replication stream reports, deciding when the buffered chunk is safe to
// emit.
//
// P is the database's position type -- whatever identifies and orders a
// committed transaction (a Postgres xid, a MySQL GTID, an Oracle SCN). Both
// methods must be pure and cheap: they run once per streamed commit.
//
// Implementations are typically small value types, so the coordinator's zero
// value for W must behave sanely: it is only consulted after the first chunk
// is planned, but must not panic before then.
type Watermark[P any] interface {
	// OpensAt reports whether a transaction committing at pos proves the
	// window has opened -- that is, pos started at or after this watermark
	// was taken, so every transaction this watermark could not see has now
	// had a chance to stream.
	OpensAt(pos P) bool

	// ClosesAt reports whether pos is strictly after every transaction that
	// could have been in flight when this watermark was taken. The
	// coordinator only closes a window once this holds for both the low and
	// the high watermark, so a chunk is never emitted while a transaction
	// that might have modified it is still unaccounted for.
	ClosesAt(pos P) bool
}
