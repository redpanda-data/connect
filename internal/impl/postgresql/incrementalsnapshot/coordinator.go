// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import "github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"

// The coordinator works with any database and is in
// internal/replication/incrementalsnapshot. These aliases select the
// Postgres position type and watermark. The call sites then do not repeat
// the type arguments.
//
// The position type is uint32, because pgoutput reports a 32-bit xid in a
// BEGIN message. This type prevents a 64-bit id that includes an epoch.
// Watermark makes the 32-bit value larger when it compares values.
type (
	// Coordinator is the incremental snapshot coordinator for Postgres.
	Coordinator = incrementalsnapshot.Coordinator[uint32, Watermark]
	// CoordinatorConfig configures a Postgres incremental snapshot Coordinator.
	CoordinatorConfig = incrementalsnapshot.CoordinatorConfig[uint32, Watermark]
	// Deps supplies the operations that have side effects.
	Deps = incrementalsnapshot.Deps[Watermark]
	// EmitFunc gets each chunk of rows that the Coordinator releases.
	EmitFunc = incrementalsnapshot.EmitFunc
)

// NewCoordinator makes a Coordinator for Postgres. If resume is not nil,
// Start continues the snapshot from that state. If resume is nil, Start
// begins a new snapshot of cfg.Tables.
func NewCoordinator(cfg CoordinatorConfig, resume *incrementalsnapshot.State) (*Coordinator, error) {
	return incrementalsnapshot.NewCoordinator(cfg, resume)
}
