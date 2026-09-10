// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

// checkpointOffset is the per-batch payload checkpointTracker tracks. Every
// run carries one, whether or not the incremental snapshot is enabled: lsn
// is what acknowledges the replication slot.
//
// incSnapshotState is the snapshot's contribution, non-nil only on a batch
// that carries a checkpoint. Riding here gives it the same acknowledgement
// ordering as the lsn.
type checkpointOffset struct {
	lsn              *string
	incSnapshotState []byte
	// seq orders the offsets; the tracker assigns it. Acknowledgements run
	// concurrently and incSnapshotState does not reveal which state is
	// newer, so the writer compares this instead.
	seq uint64
}

// merge overlays each non-nil field of other onto a copy of o.
//
// Resolving a node out of order assigns its whole payload onto its unresolved
// predecessor, so a nil field would wipe whatever the predecessor held.
// Callers must therefore merge against the last tracked payload before
// calling Track.
func (o checkpointOffset) merge(other checkpointOffset) checkpointOffset {
	merged := o
	if other.lsn != nil {
		merged.lsn = other.lsn
	}
	if other.incSnapshotState != nil {
		merged.incSnapshotState = other.incSnapshotState
	}
	if other.seq > merged.seq {
		merged.seq = other.seq
	}
	return merged
}
