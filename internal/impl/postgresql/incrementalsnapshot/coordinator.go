// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import "github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"

// The coordinator itself is database-agnostic and lives in
// internal/replication/incrementalsnapshot. These aliases pin it to
// Postgres' position type and watermark, so call sites don't repeat the
// type arguments.
//
// The position is a uint32: pgoutput reports a raw 32-bit xid on BEGIN, and
// naming that in the type stops an epoch-extended 64-bit id from being
// passed in by mistake. Watermark handles the widening.
type (
	// Coordinator is the incremental snapshot coordinator for Postgres.
	Coordinator = incrementalsnapshot.Coordinator[uint32, Watermark]
	// CoordinatorConfig configures a Postgres incremental snapshot Coordinator.
	CoordinatorConfig = incrementalsnapshot.CoordinatorConfig[uint32, Watermark]
	// Deps supplies the side-effecting operations a Postgres Coordinator needs.
	Deps = incrementalsnapshot.Deps[Watermark]
)

// NewCoordinator constructs a Postgres incremental snapshot Coordinator. If
// resume is non-nil, the coordinator picks up where that state left off once
// Start is called; otherwise it starts fresh from cfg.Tables.
func NewCoordinator(cfg CoordinatorConfig, resume *incrementalsnapshot.State) (*Coordinator, error) {
	return incrementalsnapshot.NewCoordinator(cfg, resume)
}
