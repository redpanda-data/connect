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
// read and reconciles the pair against streamed commits to decide when the
// buffered chunk is safe to emit.
//
// P is the database's position type -- whatever identifies and orders a
// committed transaction (a Postgres xid, a MySQL GTID, an Oracle SCN). Both
// methods run once per streamed commit, so must be pure and cheap, and
// neither may panic on the zero value.
type Watermark[P any] interface {
	// OpensAt reports whether pos started at or after this watermark was
	// taken, meaning everything the watermark couldn't see has had a chance
	// to stream.
	OpensAt(pos P) bool

	// ClosesAt reports whether pos is strictly after every transaction in
	// flight when this watermark was taken. The coordinator requires it of
	// both watermarks before emitting a chunk.
	ClosesAt(pos P) bool
}
