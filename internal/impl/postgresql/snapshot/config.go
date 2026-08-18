// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

import "github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"

var (
	// DefaultIncSnapshotEnabled determines whether incremental snapshotting is enabled or disabled.
	DefaultIncSnapshotEnabled = false

	DefaultIncSnapshotChunkSize = 1024

	DefaultIncSnapshotCheckpointKey = "postgres_cdc_incremental_snapshot"
)

// IncrementalSnapshotCfg configures incremental snapshotting, which runs
// automatically and concurrently with logical replication streaming once
// enabled: no signal table or trigger required.
type IncrementalSnapshotCfg struct {
	Enabled bool
	// Tables lists the (unqualified, same-schema-as-DBSchema) tables to
	// incrementally snapshot. If empty, DBTables is used instead.
	Tables    []string
	ChunkSize int
	// ResumeState, if non-nil, resumes a previously persisted incremental
	// snapshot from where it left off instead of starting fresh.
	ResumeState *incrementalsnapshot.State
}
