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
	Enabled           bool
	ChunkSize         int
	HeartbeatInterval time.Duration
	ResumeState       *incrementalsnapshot.State
}

// IsEnabled reports whether the snapshot is enabled.
func (c *Cfg) IsEnabled() bool {
	return c != nil && c.Enabled
}
