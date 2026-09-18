// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import "github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"

type (
	// Coordinator is the incremental snapshot coordinator for Postgres.
	Coordinator = incrementalsnapshot.Coordinator[uint32, Watermark]
	// CoordinatorConfig configures a Postgres incremental snapshot Coordinator.
	CoordinatorConfig = incrementalsnapshot.CoordinatorConfig[uint32, Watermark]
	// Deps supplies the side-effecting operations.
	Deps = incrementalsnapshot.Deps[Watermark]
	// EmitFunc receives each chunk of rows the Coordinator releases.
	EmitFunc = incrementalsnapshot.EmitFunc
)

// NewCoordinator builds a Postgres Coordinator. A non-nil resume makes Start
// continue from that state; otherwise it starts with an empty queue.
func NewCoordinator(cfg CoordinatorConfig, resume *incrementalsnapshot.State) (*Coordinator, error) {
	return incrementalsnapshot.NewCoordinator(cfg, resume)
}
