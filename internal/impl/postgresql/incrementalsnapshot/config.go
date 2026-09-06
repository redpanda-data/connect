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

// Cfg configures incremental snapshotting.
type Cfg struct {
	Enabled     bool
	Tables      []string
	ChunkSize   int
	ResumeState *incrementalsnapshot.State
}

// IsEnabled reports whether incremental snapshot is enabled.
func (c *Cfg) IsEnabled() bool {
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
// Resolving an out-of-order node assigns its whole payload onto its
// unresolved predecessor, so a nil field would wipe out whatever the
// predecessor already carried. Callers must merge against the last-tracked
// payload before calling Track to keep that assignment lossless.
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
