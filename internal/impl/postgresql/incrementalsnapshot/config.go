// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import "github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"

var (
	// DefaultIncSnapshotEnabled is the default for whether incremental
	// snapshotting is enabled.
	DefaultIncSnapshotEnabled = false

	// DefaultIncSnapshotChunkSize is the default row count per chunk.
	DefaultIncSnapshotChunkSize = 1024

	// DefaultIncSnapshotCheckpointKey is the default cache key used to
	// persist incremental snapshot checkpoints.
	DefaultIncSnapshotCheckpointKey = "postgres_cdc_incremental_snapshot"
)

// IncrementalSnapshotCfg configures incremental snapshotting, which runs
// automatically alongside logical replication once enabled -- no signal
// table or trigger required.
type IncrementalSnapshotCfg struct {
	Enabled bool
	// Tables to snapshot (unqualified, same schema as DBSchema). Falls
	// back to DBTables if empty.
	Tables    []string
	ChunkSize int
	// ResumeState resumes a previously persisted snapshot if non-nil.
	ResumeState *incrementalsnapshot.State
}

// IsEnabled reports whether incremental snapshot is enabled.
func (c *IncrementalSnapshotCfg) IsEnabled() bool {
	return c != nil && c.Enabled
}

// CheckpointOffset is the per-batch payload tracked by the LSN
// checkpointer. IncSnapshotState is non-nil when the batch carries a
// checkpoint, giving it the same ack-ordering as LSN.
type CheckpointOffset struct {
	LSN              *string
	IncSnapshotState []byte
}

// Merge overlays any non-nil field of other onto a copy of o.
//
// The checkpoint queue (github.com/Jeffail/checkpoint's Uncapped.Track)
// resolves an out-of-order node by assigning its entire payload onto its
// unresolved predecessor, which would wipe out a field the predecessor
// already carried if this payload's version of it is nil. Callers must
// merge each payload against the last-tracked one before calling Track
// so that assignment is a no-op for whichever field didn't advance.
func (o CheckpointOffset) Merge(other CheckpointOffset) CheckpointOffset {
	merged := o
	if other.LSN != nil {
		merged.LSN = other.LSN
	}
	if other.IncSnapshotState != nil {
		merged.IncSnapshotState = other.IncSnapshotState
	}
	return merged
}
