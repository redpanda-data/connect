// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

// Watermark shows which transactions were in flight at one instant. The
// coordinator reads one either side of a chunk read and compares the pair
// with each streamed commit to decide when the chunk can be emitted.
//
// P is the database's position type: whatever identifies and orders a
// committed transaction, such as a Postgres xid, a MySQL GTID or an Oracle
// SCN.
//
// Every method runs once per streamed commit, so each must be fast, must not
// change state, and must not panic on the zero value. Implementations must be
// comparable, because the coordinator compares a chunk's two watermarks for
// equality.
type Watermark[P any] interface {
	comparable

	// OpensAt reports whether pos started at or after this watermark, meaning
	// everything the watermark could not see has had time to stream.
	OpensAt(pos P) bool

	// ClosesAt reports whether pos follows every transaction in flight at
	// this watermark. Both watermarks must agree before a chunk is emitted.
	ClosesAt(pos P) bool

	// Quiesced reports whether nothing at all was in flight. An equal,
	// quiesced pair proves no transaction could have touched the chunk during
	// the read, which is what lets Coordinator drain it immediately.
	Quiesced() bool
}
