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

// DefaultMaxDrainChunks is the drain limit of the coordinator. It is the
// maximum number of chunks that one streamed commit can release when the
// database is quiet and needs no row removal. This package makes the value
// available here, so the input configuration does not import the shared
// replication package.
const DefaultMaxDrainChunks = incrementalsnapshot.DefaultMaxDrainChunks

// Cfg holds the incremental snapshot configuration.
type Cfg struct {
	Enabled     bool
	Tables      []string
	ChunkSize   int
	ResumeState *incrementalsnapshot.State
}

// IsEnabled tells if the incremental snapshot is enabled.
func (c *Cfg) IsEnabled() bool {
	return c != nil && c.Enabled
}

// CheckpointOffset is the data that the LSN checkpointer tracks for each
// batch. IncSnapshotState is not nil when the batch holds a checkpoint. The
// checkpoint then gets the same acknowledgement order as the LSN.
type CheckpointOffset struct {
	LSN              *string
	IncSnapshotState []byte
	// Seq orders the checkpoints. The tracker assigns it. Acknowledgements
	// can run concurrently and IncSnapshotState does not show which state is
	// newer, so the writer compares this instead.
	Seq uint64
}

// Merge copies each field of other that is not nil onto a copy of o.
//
// Track can resolve a node out of order. It then copies the full data of
// that node onto the earlier node, and a nil field would remove the value
// that the earlier node holds. Therefore the caller must merge each set of
// data with the last tracked set before it calls Track.
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
