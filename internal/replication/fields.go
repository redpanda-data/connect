// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// CheckpointLimitField returns the checkpoint_limit field of a CDC input whose
// replication stream acknowledges progress by the given kind of position, such
// as "binlog position".
func CheckpointLimitField(position string) *service.ConfigField {
	return service.NewIntField("checkpoint_limit").
		Description("The maximum number of messages that this input can process at a given time. Increasing this limit enables parallel processing, and batching at the output level. To preserve at-least-once guarantees, any given " + position + " is not acknowledged until all messages up to that position are delivered.").
		ShortDescription("The maximum number of messages that can be processed at a given time.").
		Default(1024)
}

// MaxParallelSnapshotTablesField returns the max_parallel_snapshot_tables
// field of a CDC input that reads each snapshot table with its own reader.
func MaxParallelSnapshotTablesField() *service.ConfigField {
	return service.NewIntField("max_parallel_snapshot_tables").
		Description("The maximum number of tables to read in parallel during the initial snapshot. Each table is read by its own reader.").Version("4.69.0").
		Default(1)
}
