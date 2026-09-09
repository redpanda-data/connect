// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"time"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

var (
	// DefaultIncSnapshotEnabled is the default for enabling the snapshot.
	DefaultIncSnapshotEnabled = false

	// DefaultIncSnapshotChunkSize is the default row count per chunk.
	DefaultIncSnapshotChunkSize = 1024

	// DefaultIncSnapshotCheckpointKey is the default checkpoint cache key.
	DefaultIncSnapshotCheckpointKey = "postgres_cdc_incremental_snapshot"

	// DefaultIncSnapshotHeartbeatInterval is frequent enough that the
	// backfill is bound by chunk reads rather than by the heartbeat.
	DefaultIncSnapshotHeartbeatInterval = time.Second
)

// Cfg holds the incremental snapshot configuration.
type Cfg struct {
	Enabled   bool
	Tables    []string
	ChunkSize int
	// HeartbeatInterval is how often the snapshot needs a commit to advance
	// on, separate from Config.HeartbeatInterval, which only keeps the
	// replication slot current.
	HeartbeatInterval time.Duration
	ResumeState       *incrementalsnapshot.State
}

// IsEnabled reports whether the snapshot is enabled.
func (c *Cfg) IsEnabled() bool {
	return c != nil && c.Enabled
}

// CheckpointOffset is the per-batch payload the LSN checkpointer tracks.
// IncSnapshotState is non-nil when the batch carries a checkpoint, giving it
// the same ack ordering as the LSN.
type CheckpointOffset struct {
	LSN              *string
	IncSnapshotState []byte
	// Seq orders the checkpoints; the tracker assigns it. Acknowledgements
	// run concurrently and IncSnapshotState does not reveal which state is
	// newer, so the writer compares this instead.
	Seq uint64
}

// Merge overlays each non-nil field of other onto a copy of o.
//
// Resolving a node out of order assigns its whole payload onto its unresolved
// predecessor, so a nil field would wipe whatever the predecessor held.
// Callers must therefore merge against the last tracked payload before
// calling Track.
func (o CheckpointOffset) Merge(other CheckpointOffset) CheckpointOffset {
	merged := o
	if other.LSN != nil {
		merged.LSN = other.LSN
	}
	if other.IncSnapshotState != nil {
		merged.IncSnapshotState = other.IncSnapshotState
	}
	if other.Seq > merged.Seq {
		merged.Seq = other.Seq
	}
	return merged
}
