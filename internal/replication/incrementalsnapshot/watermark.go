// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

// Watermark shows which transactions the database had in flight at one point
// in time. The coordinator reads one watermark before a chunk read and one
// after it. It then compares the pair with each streamed commit. The
// comparison tells the coordinator when it can emit the chunk.
//
// P is the position type of the database. A position identifies a committed
// transaction and puts it in order. Postgres uses a transaction id, MySQL
// uses a GTID and Oracle uses an SCN.
//
// The coordinator calls each method one time for each streamed commit.
// Therefore each method must be fast and must not change any state. No
// method must panic on the zero value.
//
// A Watermark must be comparable. The coordinator compares the two
// watermarks of a chunk read for equality.
type Watermark[P any] interface {
	comparable

	// OpensAt tells if pos started at the same time as this watermark or
	// after it. If it did, all transactions that the watermark could not see
	// have had time to stream.
	OpensAt(pos P) bool

	// ClosesAt tells if pos comes after all transactions that were in flight
	// at this watermark. The coordinator must get true from both watermarks
	// before it emits a chunk.
	ClosesAt(pos P) bool

	// Quiesced tells if the database had no transaction in flight at this
	// watermark. If both watermarks of a chunk read are quiesced and equal,
	// no transaction could change the chunk during the read. The coordinator
	// uses this result to drain chunks. Refer to Coordinator.
	Quiesced() bool
}
